################################################################################
## 1) LIBRERÍAS
################################################################################
library(rakeR)
library(RPostgres)
library(DBI)
library(sf)
library(dplyr)
library(data.table)
library(ggplot2)
library(scales)
library(units)
library(RColorBrewer)
library(rlang)
library(stringi)
library(tidyr)
library(grid)
library(cowplot)
library(biscale)

################################################################################
## 2) ENTRADAS Y BASES
################################################################################
ruta_casen <- "D:/usach/geomarketing/rds/casen_rm.rds"
ruta_censo <- "D:/usach/geomarketing/rds/cons_censo_df.rds"
casen_raw  <- readRDS(ruta_casen)
cons_censo_df <- readRDS(ruta_censo)

################################################################################
## 3) PREPROCESO CASEN
################################################################################
col_cons   <- sort(setdiff(names(cons_censo_df), c("GEOCODIGO","COMUNA")))
age_levels <- grep("^edad", col_cons, value = TRUE)
esc_levels <- grep("^esco", col_cons, value = TRUE)
sexo_levels<- grep("^sexo", col_cons, value = TRUE)

vars_base <- c("estrato","esc","edad","sexo","e6a","y7")
casen <- casen_raw[, vars_base, drop = FALSE]; rm(casen_raw)
casen$Comuna  <- substr(as.character(casen$estrato), 1, 5); casen$estrato <- NULL
casen$esc  <- as.integer(unclass(casen$esc))
casen$edad <- as.integer(unclass(casen$edad))
casen$e6a  <- as.numeric(unclass(casen$e6a))
casen$sexo <- as.integer(unclass(casen$sexo))
casen$y7   <- as.numeric(unclass(casen$y7))

# Imputación ligera de escolaridad
idx_na <- which(is.na(casen$esc))
if (length(idx_na) > 0) {
  df_fit <- casen[!is.na(casen$esc) & !is.na(casen$e6a), ]
  if (nrow(df_fit) > 5) {
    fit  <- lm(esc ~ e6a, data = df_fit)
    pred <- predict(fit, newdata = casen[idx_na, , drop = FALSE])
    casen$esc[idx_na] <- as.integer(round(pmax(0, pmin(29, pred))))
  }
}

casen$ID <- as.character(seq_len(nrow(casen)))
casen$edad_cat <- cut(casen$edad, breaks=c(0,30,40,50,60,70,80,Inf),
                      labels=age_levels, right=FALSE, include.lowest=TRUE)
casen$esc_cat  <- factor(with(casen,
                              ifelse(esc==0,esc_levels[1],
                                     ifelse(esc<=8,esc_levels[2],
                                            ifelse(esc<=12,esc_levels[3],esc_levels[4])))),
                         levels=esc_levels)
casen$sexo_cat <- factor(ifelse(casen$sexo==2,sexo_levels[1],
                                ifelse(casen$sexo==1,sexo_levels[2],NA)),
                         levels=sexo_levels)
casen$empr <- as.integer(!is.na(casen$y7) & casen$y7 > 0)

################################################################################
## 4) MICROSIMULACIÓN CON rakeR
################################################################################
cons_censo_comunas <- split(cons_censo_df, cons_censo_df$COMUNA)
inds_list          <- split(casen, casen$Comuna)
set.seed(123)

sim_list <- lapply(names(cons_censo_comunas), function(zona){
  cons_i    <- cons_censo_comunas[[zona]]
  col_order <- sort(setdiff(names(cons_i), c("COMUNA","GEOCODIGO")))
  cons_i    <- cons_i[, c("GEOCODIGO", col_order), drop=FALSE]
  tmp <- inds_list[[zona]]; if (is.null(tmp)) return(NULL)
  inds_i <- tmp[, c("ID","edad_cat","esc_cat","sexo_cat"), drop=FALSE]
  names(inds_i) <- c("ID","Edad","Escolaridad","Sexo")
  inds_i <- inds_i[complete.cases(inds_i), , drop=FALSE]
  if (nrow(inds_i)==0) return(NULL)
  w_frac <- weight(cons=cons_i, inds=inds_i, vars=c("Edad","Escolaridad","Sexo"))
  sim_i  <- integerise(weights=w_frac, inds=inds_i, seed=123)
  merge(sim_i, tmp[, c("ID","empr")], by="ID", all.x=TRUE)
})

sim_df <- data.table::rbindlist(Filter(Negate(is.null), sim_list), idcol="COMUNA", fill=TRUE)
zonas_empr <- as.data.table(sim_df)[, .(
  pct_emprendedores = mean(empr, na.rm=TRUE),
  n_emprendedores   = sum(empr, na.rm=TRUE),
  n_poblacion       = .N
), by=.(zone)]
data.table::setnames(zonas_empr, "zone", "geocodigo")
zonas_empr$geocodigo <- as.character(zonas_empr$geocodigo)

################################################################################
## 5) CONEXIÓN Y GEOMETRÍAS DESDE PostgreSQL
################################################################################
norm_str <- function(x) tolower(stringi::stri_trans_general(as.character(x), "Latin-ASCII"))
con <- dbConnect(Postgres(),
                 dbname="censo_rm_2017", host="localhost", port=5432,
                 user="postgres", password="postgres")

zonas_censales_rm <- st_read(con, query="SELECT * FROM dpa.zonas_censales_rm;", quiet=TRUE)
comunas_rm_shp    <- st_read(con, query="SELECT * FROM dpa.comunas_rm_shp;",  quiet=TRUE)
dbDisconnect(con)

names(zonas_censales_rm) <- tolower(names(zonas_censales_rm))
names(comunas_rm_shp)    <- tolower(names(comunas_rm_shp))

zonas_censales_rm <- zonas_censales_rm |>
  mutate(geocodigo = as.character(geocodigo)) |>
  left_join(zonas_empr, by="geocodigo") |>
  mutate(prop_empr = pct_emprendedores)

################################################################################
## 6) FILTRO URBANO Y RECORTE GRAN SANTIAGO
################################################################################
urb_cols <- intersect(c("urbano","es_urbano","urban","zona_urbana","urb"), names(zonas_censales_rm))
if (length(urb_cols)) {
  col_u <- urb_cols[1]
  zonas_censales_rm <- zonas_censales_rm |>
    mutate(.urb_raw = .data[[col_u]], .urb_str = norm_str(.urb_raw)) |>
    filter(.urb_raw %in% c(1, TRUE) | .urb_str %in% c("u","urbano","urban","true","si","s","1"))
}

zonas_censales_rm <- st_make_valid(zonas_censales_rm)
comunas_rm_shp    <- st_make_valid(comunas_rm_shp)
gs_comunas <- comunas_rm_shp |>
  filter(nom_provin == "SANTIAGO" | nom_comuna %in% c("SAN BERNARDO","PUENTE ALTO"))

inter <- suppressWarnings(st_intersection(st_make_valid(zonas_censales_rm),
                                          st_make_valid(gs_comunas)))
inter <- st_collection_extract(inter, "POLYGON")
inter <- inter[!st_is_empty(inter), ]

u_zonas <- st_union(inter)
buf <- tryCatch(st_buffer(st_make_valid(u_zonas), set_units(2000, "m")),
                error = function(e) st_buffer(st_make_valid(u_zonas), 2000))
bb <- st_bbox(buf)
xlim_zoom <- c(bb["xmin"], bb["xmax"])
ylim_zoom <- c(bb["ymin"], bb["ymax"])

comunas_clip <- suppressWarnings(st_intersection(gs_comunas, st_as_sf(u_zonas))) |>
  st_make_valid() |> st_collection_extract("POLYGON")
comunas_clip <- comunas_clip[!st_is_empty(comunas_clip), ] |>
  dplyr::group_by(nom_comuna) |> dplyr::summarise(.groups="drop")

################################################################################
## 7) TEMA Y PALETA
################################################################################
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
      legend.title     = element_text(face = "bold", margin = margin(b = 8)),
      legend.key.height= unit(14, "pt"),
      legend.key.width = unit(10, "pt"),
      plot.margin      = margin(10, 40, 10, 10)
    )
}

################################################################################
## 8) MAPA DE EMPRENDEDORES (%)
################################################################################
inter$prop_pct <- 100 * inter$prop_empr
max_plot <- 18.8

p_empr <- ggplot(inter) +
  geom_sf(aes(fill = prop_pct),
          color = alpha("white", 0.6), linewidth = 0.15) +
  geom_sf(data = comunas_clip, fill = NA,
          color = "grey25", linewidth = 0.45) +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE,
           label_graticule = "SW", label_axes = "SW") +
  scale_fill_gradientn(colors = pal_blues,
                       limits = c(0, max_plot),
                       oob = squish,
                       name = "% con emprendimientos",
                       breaks = c(0,5,10,15,18.8),
                       labels = function(x) gsub("\\.", ",", sprintf("%.1f", x))) +
  guides(fill = guide_colorbar(barheight = unit(120, "pt"),
                               title.position = "top", title.hjust = 0.5)) +
  labs(title = "Población con actividad emprendedora (%)",
       subtitle = "Zona urbana del Gran Santiago por zona censal",
       caption = "Fuente: CASEN (rakeR) + Censo 2017") +
  theme_joaquín()

ggsave("mapa_emprendedores_urbano.png", p_empr, width = 9, height = 7, dpi = 300)
print(p_empr)

################################################################################
## 9) DISTRIBUCIÓN DE TIPO DE VIVIENDA (CASAS / DEPTOS)
################################################################################
con <- dbConnect(Postgres(),
                 dbname="censo_rm_2017", host="localhost", port=5432,
                 user="postgres", password="postgres")

sql_tipo_viv <- "
WITH base AS (
  SELECT
    z.geocodigo::text AS geocodigo,
    c.nom_comuna,
    v.p01,
    v.p02
  FROM public.viviendas v
  JOIN public.zonas   z ON v.zonaloc_ref_id = z.zonaloc_ref_id
  JOIN public.comunas c ON z.codigo_comuna  = c.codigo_comuna
),
viviendas_validas AS (
  SELECT *
  FROM base
  WHERE p02 = 1  -- solo viviendas particulares ocupadas
),
agg AS (
  SELECT
    geocodigo,
    MAX(nom_comuna) AS nom_comuna,
    COUNT(*) AS total_viv,
    COUNT(*) FILTER (WHERE p01 = 1) AS n_casas,
    COUNT(*) FILTER (WHERE p01 = 2) AS n_deptos
  FROM viviendas_validas
  GROUP BY geocodigo
)
SELECT
  geocodigo,
  nom_comuna,
  ROUND(100.0 * n_casas  / NULLIF(total_viv,0), 2) AS pct_casas,
  ROUND(100.0 * n_deptos / NULLIF(total_viv,0), 2) AS pct_deptos
FROM agg;
"

tbl_viv <- dbGetQuery(con, sql_tipo_viv) %>%
  mutate(geocodigo = as.character(geocodigo))
dbDisconnect(con)

sf_viv <- inter %>%
  mutate(geocodigo = as.character(geocodigo)) %>%
  left_join(tbl_viv, by = "geocodigo")

################################################################################
## 10) MAPAS – % CASAS Y % DEPARTAMENTOS 
################################################################################
p_casas <- ggplot(sf_viv) +
  geom_sf(aes(fill = pct_casas),
          color = alpha("white", 0.6), linewidth = 0.15) +
  geom_sf(data = comunas_clip, fill = NA,
          color = "grey25", linewidth = 0.45) +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE,
           label_graticule = "SW", label_axes = "SW") +
  scale_fill_gradientn(
    colors = pal_blues,
    limits = c(0, 100),
    oob = squish,
    name = "% de viviendas tipo casa",
    breaks = c(0, 25, 50, 75, 100),
    labels = label_number(accuracy = 1, decimal.mark = ",")
  ) +
  guides(fill = guide_colorbar(barheight = unit(120, "pt"),
                               title.position = "top", title.hjust = 0.5)) +
  labs(title = "Distribución de viviendas tipo casa (%)",
       subtitle = "Zona urbana del Gran Santiago por zona censal",
       caption = "Fuente: Censo 2017, elaboración propia") +
  theme_joaquín()

p_deptos <- ggplot(sf_viv) +
  geom_sf(aes(fill = pct_deptos),
          color = alpha("white", 0.6), linewidth = 0.15) +
  geom_sf(data = comunas_clip, fill = NA,
          color = "grey25", linewidth = 0.45) +
  coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE,
           label_graticule = "SW", label_axes = "SW") +
  scale_fill_gradientn(
    colors = pal_blues,
    limits = c(0, 100),       # ← fuerza el rango de 0–100%
    oob = squish,             # ← evita cortes si hay valores fuera
    name = "% de viviendas tipo departamento",
    breaks = c(0, 25, 50, 75, 100),   # ← asegura que aparezca 100%
    labels = label_number(accuracy = 1, decimal.mark = ",")
  ) +
  guides(fill = guide_colorbar(barheight = unit(120, "pt"),
                               title.position = "top", title.hjust = 0.5)) +
  labs(title = "Distribución de viviendas tipo departamento (%)",
       subtitle = "Zona urbana del Gran Santiago por zona censal",
       caption = "Fuente: Censo 2017, elaboración propia") +
  theme_joaquín()

ggsave("mapa_viviendas_tipo_casa.png", p_casas, width = 9, height = 7, dpi = 300)
ggsave("mapa_viviendas_tipo_departamento.png", p_deptos, width = 9, height = 7, dpi = 300)

print(p_casas)
print(p_deptos)

################################################################################
## 11) MAPAS BIVARIADOS 3×3 – Emprendedores vs Casas / Departamentos (CORREGIDO)
################################################################################
library(biscale)
library(cowplot)

# Base con las tres variables ya en porcentaje
sf_bi_base <- sf_viv |>
  dplyr::mutate(
    empr_pct   = prop_pct,     # % emprendedores (viene de 'inter')
    casas_pct  = pct_casas,    # % casas
    deptos_pct = pct_deptos    # % deptos
  )

# Helper robusto: recibe nombres de columnas como strings
make_bi <- function(sf_data, var_x, var_y, xlab, ylab, title){
  # crear columnas canon x/y y filtrar NA/Inf
  df_xy <- sf_data |>
    dplyr::mutate(
      x = .data[[var_x]],
      y = .data[[var_y]]
    ) |>
    dplyr::filter(is.finite(x), is.finite(y))
  
  # clasificar en 3×3 cuantiles
  sf_bi <- biscale::bi_class(
    df_xy,
    x = x, y = y,
    dim = 3, style = "quantile"
  )
  
  # mapa principal
  p_map <- ggplot(sf_bi) +
    geom_sf(aes(fill = bi_class),
            color = scales::alpha("white", 0.6),
            linewidth = 0.15,
            show.legend = FALSE) +
    geom_sf(data = comunas_clip, fill = NA,
            color = "grey25", linewidth = 0.45) +
    biscale::bi_scale_fill(pal = "DkBlue", dim = 3) +
    coord_sf(xlim = xlim_zoom, ylim = ylim_zoom, expand = FALSE,
             label_graticule = "SW", label_axes = "SW") +
    labs(
      title    = title,
      subtitle = "Zona urbana del Gran Santiago por zona censal",
      caption  = "Clasificación bivariada (cuantiles 3×3) • Fuente: CASEN (rakeR) + Censo 2017"
    ) +
    theme_joaquín()
  
  # leyenda (texto + overlay de flecha horizontal)
  p_leg <- biscale::bi_legend(
    pal = "DkBlue", dim = 3,
    xlab = xlab,                                  # ← sin flecha aquí
    ylab = paste0("\u2191 ", ylab),               # ↑ mantiene vertical
    size = 10
  ) +
    theme_minimal(base_size = 10) +
    theme(
      panel.grid   = element_blank(),
      axis.text    = element_blank(),
      axis.ticks   = element_blank(),
      axis.title.x = element_text(margin = margin(t = 6)),
      axis.title.y = element_text(margin = margin(r = 6)),
      plot.margin  = margin(6, 6, 6, 6)
    )
  
  # composición mapa + leyenda, con overlay de flecha "→" sobre la leyenda
  composed <- cowplot::ggdraw() +
    cowplot::draw_plot(p_map, x = 0.00, y = 0.00, width = 0.80, height = 1.00) +
    cowplot::draw_plot(p_leg, x = 0.82, y = 0.18, width = 0.18, height = 0.28) +
    # ⟶ Flecha añadida como etiqueta para evitar problemas de Unicode del device
    cowplot::draw_label(
      label = "\u2192",          # → flecha derecha
      x = 0.82 + 0.09,           # centrada dentro del recuadro de la leyenda
      y = 0.18 + 0.02,           # justo debajo del triángulo de la leyenda
      hjust = 0.5, vjust = 0.5,
      size = 10
    )
  
  
  
  # composición mapa + leyenda
  composed <- cowplot::ggdraw() +
    cowplot::draw_plot(p_map, x = 0.00, y = 0.00, width = 0.80, height = 1.00) +
    cowplot::draw_plot(p_leg, x = 0.82, y = 0.18, width = 0.18, height = 0.28)
  
  list(map = p_map, legend = p_leg, composed = composed)
}

# 11.1) Emprendedores vs Casas
bi_casas <- make_bi(
  sf_data = sf_bi_base,
  var_x   = "empr_pct",
  var_y   = "casas_pct",
  xlab    = "% Emprendedores",
  ylab    = "% Casas",
  title   = "Bivariado: % Emprendedores vs. % Casas"
)
ggsave("mapa_bivariado_empr_vs_casas.png",
       bi_casas$composed, width = 9.5, height = 7.5, dpi = 300)
print(bi_casas$composed)

# 11.2) Emprendedores vs Departamentos
bi_deptos <- make_bi(
  sf_data = sf_bi_base,
  var_x   = "empr_pct",
  var_y   = "deptos_pct",
  xlab    = "% Emprendedores",
  ylab    = "% Departamentos",
  title   = "Bivariado: % Emprendedores vs. % Departamentos"
)
ggsave("mapa_bivariado_empr_vs_deptos.png",
       bi_deptos$composed, width = 9.5, height = 7.5, dpi = 300)
print(bi_deptos$composed)


