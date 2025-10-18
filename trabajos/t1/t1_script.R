#########################
## 1) Librerías   ##
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
library(scales)
library(grid)

#################################
## 2) Conexión a PostgreSQL    ##
#################################
con <- dbConnect(
  Postgres(),
  dbname   = "censo_rm_2017",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = "postgres"
)

######################################################
## 3) Lectura de capas espaciales (sf)              ##
######################################################
zonas_censales_rm <- st_read(con, query = "SELECT * FROM dpa.zonas_censales_rm;")
comunas_rm_shp    <- st_read(con, query = "SELECT * FROM dpa.comunas_rm_shp;")
names(zonas_censales_rm) <- tolower(names(zonas_censales_rm))
names(comunas_rm_shp)    <- tolower(names(comunas_rm_shp))

######################################################
## 4) Definiciones auxiliares       ##
######################################################
norm <- function(x){ x <- iconv(x, to = "ASCII//TRANSLIT"); tolower(trimws(x)) }

# Asegurar columna de nombre de comuna
if (!"nom_comuna" %in% names(comunas_rm_shp)) {
  alt <- intersect(c("nombre","comuna","name"), names(comunas_rm_shp))
  comunas_rm_shp <- rename(comunas_rm_shp, nom_comuna = !!alt[1])
}

# Detectar columna de provincia y crear estandarizados
prov_col <- intersect(c("nom_provin","nom_provincia","provincia"), names(comunas_rm_shp))
comunas_rm_shp <- comunas_rm_shp %>%
  mutate(nom_std  = norm(nom_comuna),
         prov_std = norm(.data[[prov_col[1]]]))

######################################################
## 5) Delimitación del área de estudio:             ##
##    Gran Santiago (Prov. Santiago + SB + PA)      ##
######################################################
extra_comunas <- norm(c("San Bernardo","Puente Alto"))
sf_comunas <- comunas_rm_shp %>%
  filter(prov_std == "santiago" | nom_std %in% extra_comunas)

# Homologar CRS si difiere
if (!st_crs(zonas_censales_rm) == st_crs(sf_comunas)) {
  sf_comunas <- st_transform(sf_comunas, st_crs(zonas_censales_rm))
}

######################################################
## 6) Filtrado urbano y recorte por comunas         ##
######################################################
urb_cols <- intersect(c("urbano","es_urbano","urban","zona_urbana","urb"), names(zonas_censales_rm))
if (length(urb_cols)) {
  zonas_censales_rm <- zonas_censales_rm %>%
    mutate(.urb_ = !!sym(urb_cols[1])) %>%
    filter(.urb_ %in% c(1, TRUE) | norm(as.character(.urb_)) %in% c("u","urbano","urban"))
}
# Zonas censales únicamente dentro de Gran Santiago
sf_zonas_tgt <- st_join(zonas_censales_rm, sf_comunas[, 0], join = st_intersects, left = FALSE)

######################################################
## 7) Parámetros de zoom, bordes y etiquetas        ##
######################################################
u_zonas <- st_union(sf_zonas_tgt)
bb <- st_bbox(st_buffer(u_zonas, dist = 2000))
xlim_zoom <- c(bb["xmin"], bb["xmax"])
ylim_zoom <- c(bb["ymin"], bb["ymax"])

comunas_clip <- suppressWarnings(st_intersection(sf_comunas, st_as_sf(u_zonas))) %>%
  group_by(nom_comuna) %>%
  summarise(.groups = "drop") %>%
  mutate(nom_std = norm(nom_comuna))

bordes_comunas <- st_cast(st_boundary(comunas_clip), "MULTILINESTRING")

# Etiquetas centrales por comuna (si las necesitas)
labs_all <- st_point_on_surface(comunas_clip) %>%
  mutate(nom_std = norm(nom_comuna),
         lbl = toupper(nom_comuna))
especiales <- c("san miguel","san joaquin","pedro aguirre cerda")
labs_otros <- labs_all %>% filter(!nom_std %in% especiales)
lab_sm  <- labs_all %>% filter(nom_std == "san miguel")        %>% cbind(st_coordinates(.))
lab_sj  <- labs_all %>% filter(nom_std == "san joaquin")       %>% cbind(st_coordinates(.))
lab_pac <- labs_all %>% filter(nom_std == "pedro aguirre cerda") %>% cbind(st_coordinates(.))
set.seed(123)

######################################################
## 8) Consulta SQL y construcción de indicadores    ##
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

tbl_ind <- dbGetQuery(con, sql_ind_rm) %>% mutate(geocodigo = as.character(geocodigo))
sf_ind  <- sf_zonas_tgt %>% mutate(geocodigo = as.character(geocodigo)) %>% left_join(tbl_ind, by = "geocodigo")

######################################################
## 9) Paleta y diseño               ##
######################################################
pal_blues <- c("#f7fbff","#deebf7","#c6dbef","#9ecae1","#6baed6","#3182bd","#08519c")

theme_joaquín <- function() {
  theme_minimal(base_size = 12) +
    theme(
      panel.grid.major = element_line(color = "grey85", linewidth = 0.3),
      panel.grid.minor = element_blank(),
      axis.text        = element_text(color = "grey30"),
      axis.title       = element_blank(),
      axis.ticks       = element_blank(),
      plot.title       = element_text(face = "bold", size = 14),
      plot.subtitle    = element_text(color = "grey30", size = 11),
      plot.caption     = element_text(size = 8, color = "grey40"),
      legend.position  = "right",
      legend.justification = "center",
      legend.title     = element_text(face = "bold"),
      legend.key.height = unit(14, "pt"),
      legend.key.width  = unit(10, "pt"),
      plot.margin      = margin(10, 40, 10, 10)
    )
}

######################################################
## 10) Histogramas y dispersión (cuartiles)         ##
######################################################
df_plot <- sf_ind %>%
  st_drop_geometry() %>%
  select(pct_25_39, tam_med_hogar) %>%
  filter(is.finite(pct_25_39), is.finite(tam_med_hogar))

p_hist_25_39 <- ggplot(df_plot, aes(x = pct_25_39)) +
  geom_histogram(bins = 30, fill = "#3182bd", color = "white") +
  labs(title = "Distribución: % población 25–39", x = "% 25–39", y = "Zonas censales") +
  theme_minimal(base_size = 12)

p_hist_hogar <- ggplot(df_plot, aes(x = tam_med_hogar)) +
  geom_histogram(bins = 30, fill = "#6baed6", color = "white") +
  labs(title = "Distribución: tamaño medio del hogar", x = "Personas por hogar", y = "Zonas censales") +
  theme_minimal(base_size = 12)

# Cuartiles y clasificación por cuadrantes
qx <- quantile(df_plot$pct_25_39,     probs = c(.25, .5, .75), na.rm = TRUE)
qy <- quantile(df_plot$tam_med_hogar, probs = c(.25, .5, .75), na.rm = TRUE)

df_bi <- df_plot %>%
  mutate(
    cuadrante = case_when(
      pct_25_39 >= qx[2] & tam_med_hogar >= qy[2] ~ "Q1: % 25–39 alto / Hogar grande",
      pct_25_39 >= qx[2] & tam_med_hogar <  qy[2] ~ "Q2: % 25–39 alto / Hogar pequeño",
      pct_25_39 <  qx[2] & tam_med_hogar <  qy[2] ~ "Q3: % 25–39 bajo / Hogar pequeño",
      TRUE                                        ~ "Q4: % 25–39 bajo / Hogar grande"
    )
  )

col_bi <- c(
  "Q1: % 25–39 alto / Hogar grande"   = "#3b4994",
  "Q2: % 25–39 alto / Hogar pequeño"  = "#be64ac",
  "Q3: % 25–39 bajo / Hogar pequeño"  = "#b5b5b5",
  "Q4: % 25–39 bajo / Hogar grande"   = "#5ac8c8"
)

p_scatter <- ggplot(df_bi, aes(x = pct_25_39, y = tam_med_hogar)) +
  annotate("rect", xmin = -Inf, xmax = qx[2], ymin = -Inf, ymax = qy[2], fill = "#b5b5b5", alpha = .10) +
  annotate("rect", xmin =  qx[2], xmax =  Inf, ymin = -Inf, ymax = qy[2], fill = "#be64ac", alpha = .10) +
  annotate("rect", xmin = -Inf, xmax = qx[2], ymin =  qy[2], ymax =  Inf, fill = "#5ac8c8", alpha = .10) +
  annotate("rect", xmin =  qx[2], xmax =  Inf, ymin =  qy[2], ymax =  Inf, fill = "#3b4994", alpha = .10) +
  geom_vline(xintercept = qx[2], linetype = 2, color = "grey40") +
  geom_hline(yintercept = qy[2], linetype = 2, color = "grey40") +
  geom_vline(xintercept = c(qx[1], qx[3]), linetype = 3, color = "grey70", linewidth = .4) +
  geom_hline(yintercept = c(qy[1], qy[3]), linetype = 3, color = "grey70", linewidth = .4) +
  geom_point(aes(color = cuadrante), alpha = .9, size = 2) +
  scale_color_manual(values = col_bi, name = "Cuadrante") +
  labs(title = "Dispersión bivariada por cuartiles",
       x = "% población 25–39", y = "Personas por hogar") +
  theme_minimal(base_size = 12)

######################################################
## 11) Mapas univariados (% 25–39 y tamaño hogar)  ##
######################################################
p_map_25_39 <- ggplot(sf_ind) +
  geom_sf(aes(fill = pct_25_39), color = alpha("white", 0.6), linewidth = 0.15) +
  geom_sf(data = bordes_comunas, fill = NA, color = "grey25", linewidth = 0.45) +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE,
           label_graticule = "SW", label_axes = "SW") +
  scale_fill_gradientn(colors = pal_blues, name = "% 25–39",
                       breaks = pretty_breaks(6),
                       labels = label_number(accuracy = 1, decimal.mark = ",")) +
  guides(fill = guide_colorbar(barheight = unit(120, "pt"),
                               title.position = "top", title.hjust = 0.5)) +
  labs(title = "Población 25–39 años por zona censal",
       subtitle = "Gran Santiago",
       caption = "Fuente: Censo 2017, elaboración propia") +
  theme_joaquín()

p_map_hogar <- ggplot(sf_ind) +
  geom_sf(aes(fill = tam_med_hogar), color = alpha("white", 0.6), linewidth = 0.15) +
  geom_sf(data = bordes_comunas, fill = NA, color = "grey25", linewidth = 0.45) +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE,
           label_graticule = "SW", label_axes = "SW") +
  scale_fill_gradientn(colors = pal_blues, name = "Personas por hogar",
                       breaks = pretty_breaks(6),
                       labels = label_number(accuracy = 0.1, decimal.mark = ",")) +
  guides(fill = guide_colorbar(barheight = unit(120, "pt"),
                               title.position = "top", title.hjust = 0.5)) +
  labs(title = "Tamaño medio del hogar por zona censal",
       subtitle = "Gran Santiago",
       caption = "Fuente: Censo 2017, elaboración propia") +
  theme_joaquín()

######################################################
## 12) Mapa bivariado (3×3 cuantiles) + leyenda     ##
######################################################
sf_bi_map <- biscale::bi_class(sf_ind, x = pct_25_39, y = tam_med_hogar, dim = 3, style = "quantile")

map_bi_core <- ggplot(sf_bi_map) +
  geom_sf(aes(fill = bi_class), color = alpha("white", 0.6), linewidth = 0.15, show.legend = FALSE) +
  geom_sf(data = bordes_comunas, fill = NA, color = "grey25", linewidth = 0.45) +
  biscale::bi_scale_fill(pal = "DkBlue", dim = 3) +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE,
           label_graticule = "SW", label_axes = "SW") +
  theme_joaquín()

legend_core <- biscale::bi_legend(
  pal  = "DkBlue",
  dim  = 3,
  xlab = "% 25–39 \u2192",
  ylab = "↑ Tamaño del hogar",
  size = 10
) +
  theme_minimal(base_size = 10) +
  theme(
    panel.grid   = element_blank(),
    axis.text    = element_blank(),
    axis.ticks   = element_blank(),
    axis.title.x = element_text(margin = margin(t = 6)),
    axis.title.y = element_text(margin = margin(r = 6)),
    plot.margin  = margin(8, 8, 8, 8)
  )

p_map_bivariado <- cowplot::ggdraw() +
  cowplot::draw_plot(map_bi_core,  x = 0.00, y = 0.06, width = 0.78, height = 0.90) +
  cowplot::draw_plot(legend_core,  x = 0.80, y = 0.30, width = 0.17, height = 0.25) +
  cowplot::draw_label("Relación bivariada: % 25–39 vs. Tamaño medio del hogar",
                      x = 0.02, y = 0.98, hjust = 0, vjust = 1, size = 16, fontface = "bold") +
  cowplot::draw_label("Gran Santiago",
                      x = 0.02, y = 0.94, hjust = 0, vjust = 1, size = 12, color = "grey30") +
  cowplot::draw_label("Clasificación cuantílica (3×3) • Fuente: Censo 2017, elaboración propia",
                      x = 0.50, y = 0.02, hjust = 0.5, vjust = 0, size = 9, color = "grey40")

######################################################
## 13) Prints                ##
######################################################
print(p_hist_25_39)
print(p_hist_hogar)
print(p_scatter)
print(p_map_25_39)
print(p_map_hogar)
print(p_map_bivariado)
