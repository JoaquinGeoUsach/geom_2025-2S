######################### 
## 1) Librerías base  ##
#########################
library(DBI)
library(RPostgres)
library(sf)
library(dplyr)
library(ggplot2)
library(RColorBrewer)
library(cowplot)
library(biscale)
library(viridis)
library(ggrepel)
library(rlang)

############################
## 2) Conexión a Postgres ##
############################
con <- dbConnect(
  Postgres(),
  dbname   = "censo_rm_2017",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = "postgres"
)

######################################################
## 3) Leer capas (sf)                              ##
######################################################
zonas_censales_rm <- st_read(con, query = "SELECT * FROM dpa.zonas_censales_rm;")
comunas_rm_shp    <- st_read(con, query = "SELECT * FROM dpa.comunas_rm_shp;")

names(zonas_censales_rm) <- tolower(names(zonas_censales_rm))
names(comunas_rm_shp)    <- tolower(names(comunas_rm_shp))

######################################################
## 4) Filtro: Provincia de Santiago + SB + PA      ##
######################################################
norm <- function(x){ x <- iconv(x, to="ASCII//TRANSLIT"); tolower(trimws(x)) }

if (!"nom_comuna" %in% names(comunas_rm_shp)) {
  alt <- intersect(c("nombre","comuna","name"), names(comunas_rm_shp))
  comunas_rm_shp <- rename(comunas_rm_shp, nom_comuna = !!alt[1])
}

prov_col <- intersect(c("nom_provin","nom_provincia","provincia"), names(comunas_rm_shp))
comunas_rm_shp <- comunas_rm_shp |>
  mutate(nom_std  = norm(nom_comuna),
         prov_std = norm(.data[[prov_col[1]]]))

extra_comunas <- norm(c("San Bernardo","Puente Alto"))
sf_comunas <- comunas_rm_shp |>
  filter(prov_std == "santiago" | nom_std %in% extra_comunas)

if (!st_crs(zonas_censales_rm) == st_crs(sf_comunas)) {
  sf_comunas <- st_transform(sf_comunas, st_crs(zonas_censales_rm))
}

######################################################
## 5) Zonas urbanas y recorte espacial              ##
######################################################
urb_cols <- intersect(c("urbano","es_urbano","urban","zona_urbana","urb"), names(zonas_censales_rm))
if (length(urb_cols)) {
  zonas_censales_rm <- zonas_censales_rm |>
    mutate(.urb_ = !!sym(urb_cols[1])) |>
    filter(.urb_ %in% c(1, TRUE) | norm(as.character(.urb_)) %in% c("u","urbano","urban"))
}

sf_zonas_tgt <- st_join(zonas_censales_rm, sf_comunas[,0], join = st_intersects, left = FALSE)

######################################################
## 6) Consulta SQL                                  ##
######################################################
sql_ind_rm <- "
WITH base AS (
  SELECT
    z.geocodigo::text AS geocodigo,
    c.nom_comuna,
    p.p09, p.p07, h.hogar_ref_id
  FROM public.personas p
  JOIN public.hogares h  ON p.hogar_ref_id    = h.hogar_ref_id
  JOIN public.viviendas v ON h.vivienda_ref_id = v.vivienda_ref_id
  JOIN public.zonas z     ON v.zonaloc_ref_id  = z.zonaloc_ref_id
  JOIN public.comunas c   ON z.codigo_comuna   = c.codigo_comuna
),
particulares AS (
  SELECT * FROM base
  WHERE p07 NOT IN (17,18,19)
    AND p09 NOT IN (131,132)
),
agg AS (
  SELECT
    geocodigo,
    MAX(nom_comuna) AS nom_comuna,
    ROUND(COUNT(*) FILTER (WHERE p09 BETWEEN 25 AND 39) * 100.0 / NULLIF(COUNT(*),0), 2) AS pct_25_39,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT hogar_ref_id),0), 2) AS tam_med_hogar
  FROM particulares
  GROUP BY geocodigo
)
SELECT * FROM agg;
"

tbl_ind <- dbGetQuery(con, sql_ind_rm) |> mutate(geocodigo = as.character(geocodigo))
sf_ind <- sf_zonas_tgt |> mutate(geocodigo = as.character(geocodigo)) |> left_join(tbl_ind, by="geocodigo")

######################################################
## 7) Zoom y etiquetas                              ##
######################################################
u_zonas <- st_union(sf_ind)
bb <- st_bbox(st_buffer(u_zonas, dist = 1500))
xlim_zoom <- c(bb["xmin"], bb["xmax"])
ylim_zoom <- c(bb["ymin"], bb["ymax"])

comunas_clip <- suppressWarnings(st_intersection(sf_comunas, st_as_sf(u_zonas))) |>
  group_by(nom_comuna) |>
  summarise(.groups="drop") |>
  mutate(nom_std = norm(nom_comuna))

bordes_comunas <- st_cast(st_boundary(comunas_clip), "MULTILINESTRING")

# Etiquetas centradas base
labs_all <- st_point_on_surface(comunas_clip) |>
  mutate(nom_std = norm(nom_comuna),
         lbl = toupper(nom_comuna))

# Comunas con repel (evitar sobreposición)
especiales <- c("san miguel","san joaquin","pedro aguirre cerda")
labs_otros <- labs_all |> filter(!nom_std %in% especiales)

# Filtrar y luego agregar coords (evita error del '.')
lab_sm  <- labs_all |> filter(nom_std == "san miguel")
lab_sj  <- labs_all |> filter(nom_std == "san joaquin")
lab_pac <- labs_all |> filter(nom_std == "pedro aguirre cerda")

lab_sm  <- cbind(lab_sm,  st_coordinates(lab_sm))
lab_sj  <- cbind(lab_sj,  st_coordinates(lab_sj))
lab_pac <- cbind(lab_pac, st_coordinates(lab_pac))

set.seed(123) # reproducibilidad de ggrepel

######################################################
## 8) Histogramas + dispersión                      ##
######################################################
df_plot <- sf_ind |>
  st_drop_geometry() |>
  select(pct_25_39, tam_med_hogar) |>
  filter(!is.na(pct_25_39), !is.na(tam_med_hogar))

ggplot(df_plot, aes(x = pct_25_39)) +
  geom_histogram(bins = 30, fill = "#cf4446", color = "white") +
  labs(title = "Distribución de población etaria 25–39",
       x = "Porcentaje población etaria 25–39",
       y = "Cantidad de zonas censales") +
  theme_minimal()

ggplot(df_plot, aes(x = tam_med_hogar)) +
  geom_histogram(bins = 30, fill = "#2c7fb8", color = "white") +
  labs(title = "Distribución tamaño medio del hogar",
       x = "Personas por hogar",
       y = "Cantidad de zonas censales (N°)") +
  theme_minimal()

ggplot(df_plot, aes(x = pct_25_39, y = tam_med_hogar)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", se = FALSE, linewidth = 1, color="#1b5e20") +
  labs(title = "Relación: % Etario 25–39 vs Tamaño medio del hogar",
       x = "Porcentaje población etaria 25–39 (%)",
       y = "Personas por hogar") +
  theme_minimal()

######################################################
## 9) Mapas                                         ##
######################################################
p1 <- ggplot() +
  geom_sf(data = sf_ind, aes(fill = pct_25_39), color = "white", size = 0.25) +
  geom_sf(data = bordes_comunas, color = "black", size = 0.6) +
  geom_sf_text(data = labs_otros, aes(label = lbl), size = 2.2, fontface = "bold", check_overlap = TRUE) +
  # Etiquetas con repel para las 3 comunas conflictivas
  geom_text_repel(data = lab_sm,  aes(x = X, y = Y, label = "SAN MIGUEL"),
                  nudge_y = -350, box.padding = 0.4, point.padding = 0.5,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  geom_text_repel(data = lab_sj,  aes(x = X, y = Y, label = "SAN JOAQUIN"),
                  nudge_y =  350, nudge_x = 120, box.padding = 0.4, point.padding = 0.5,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  geom_text_repel(data = lab_pac, aes(x = X, y = Y, label = "PEDRO AGUIRRE CERDA"),
                  nudge_x = -450, nudge_y = 120, box.padding = 0.5, point.padding = 0.6,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  scale_fill_distiller(palette = "Reds", direction = 1, name = "% 25–39") +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE) +
  theme_void() +
  labs(title = "% Población etaria 25–39 por zona censal",
       subtitle = "Provincia de Santiago + San Bernardo + Puente Alto")

p2 <- ggplot() +
  geom_sf(data = sf_ind, aes(fill = tam_med_hogar), color = "white", size = 0.25) +
  geom_sf(data = bordes_comunas, color = "black", size = 0.6) +
  geom_sf_text(data = labs_otros, aes(label = lbl), size = 2.2, fontface = "bold", check_overlap = TRUE) +
  geom_text_repel(data = lab_sm,  aes(x = X, y = Y, label = "SAN MIGUEL"),
                  nudge_y = -350, box.padding = 0.4, point.padding = 0.5,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  geom_text_repel(data = lab_sj,  aes(x = X, y = Y, label = "SAN JOAQUIN"),
                  nudge_y =  350, nudge_x = 120, box.padding = 0.4, point.padding = 0.5,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  geom_text_repel(data = lab_pac, aes(x = X, y = Y, label = "PEDRO AGUIRRE CERDA"),
                  nudge_x = -450, nudge_y = 120, box.padding = 0.5, point.padding = 0.6,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  scale_fill_distiller(palette = "YlGnBu", direction = 1, name = "Pers./hogar") +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE) +
  theme_void() +
  labs(title = "Tamaño medio del hogar por zona censal",
       subtitle = "Provincia de Santiago + San Bernardo + Puente Alto")

p1; p2

######################################################
## 10) Bivariado (alto contraste)                   ##
######################################################
sf_bi <- bi_class(sf_ind, x = pct_25_39, y = tam_med_hogar, dim = 3, style = "quantile")

mapa_bi <- ggplot() +
  geom_sf(data = sf_bi, aes(fill = bi_class), color = "white", size = 0.25, show.legend = FALSE) +
  geom_sf(data = bordes_comunas, color = "black", size = 0.6) +
  geom_sf_text(data = labs_otros, aes(label = lbl), size = 2.2, fontface = "bold", check_overlap = TRUE) +
  geom_text_repel(data = lab_sm,  aes(x = X, y = Y, label = "SAN MIGUEL"),
                  nudge_y = -350, box.padding = 0.4, point.padding = 0.5,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  geom_text_repel(data = lab_sj,  aes(x = X, y = Y, label = "SAN JOAQUIN"),
                  nudge_y =  350, nudge_x = 20, box.padding = 0.4, point.padding = 0.5,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  geom_text_repel(data = lab_pac, aes(x = X, y = Y, label = "PEDRO AGUIRRE CERDA"),
                  nudge_x = 0, nudge_y = 30, box.padding = 0.5, point.padding = 0.6,
                  min.segment.length = 0, max.overlaps = Inf,
                  size = 2.2, fontface = "bold") +
  bi_scale_fill(pal = "DkBlue", dim = 3) +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE) +
  theme_void() +
  labs(title = "% Poblacion etaria 25–39 vs Tamaño medio del hogar",
       subtitle = "Provincia de Santiago + San Bernardo + Puente Alto")

leyenda_bi <- bi_legend(
  pal = "DkBlue", dim = 3,
  xlab = "% 25–39 (bajo → alto)",
  ylab = "Pers./hogar (bajo → alto)",
  size = 7
)

cowplot::ggdraw() +
  cowplot::draw_plot(mapa_bi, x = 0, y = 0, width = 1, height = 1) +
  cowplot::draw_plot(leyenda_bi, x = 0.72, y = 0.05, width = 0.26, height = 0.26)
