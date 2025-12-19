# --- CARGA DE LIBRERÍAS ---
library(haven)
library(pROC)
library(mgcv)
library(ggplot2)

# --- CARGA DE DATOS EPF ---
personas   <- read_dta("C:/Users/Usuario/Documents/geom_2025-2S/datos_epf/base-personas-ix-epf-stata.dta")
gastos     <- read_dta("C:/Users/Usuario/Documents/geom_2025-2S/datos_epf/base-gastos-ix-epf-stata.dta")
cantidades <- read_dta("C:/Users/Usuario/Documents/geom_2025-2S/datos_epf/base-cantidades-ix-epf-stata.dta")
ccif       <- read_dta("C:/Users/Usuario/Documents/geom_2025-2S/datos_epf/ccif-ix-epf-stata.dta")

# --- FILTRADO: Gran Santiago y datos válidos ---
valores_invalidos <- c(-99, -88, -77)

personas_gs <- subset(
  personas,
  macrozona == 2 &
    !(edad %in% valores_invalidos) &
    !(edue %in% valores_invalidos) &
    ing_disp_hog_hd_ai >= 0
)

# --- VARIABLES DERIVADAS ---
personas_gs$ing_pc <- personas_gs$ing_disp_hog_hd_ai / personas_gs$npersonas
personas_gs$id_persona <- paste(personas_gs$folio, personas_gs$n_linea, sep = "_")
cantidades$id_persona  <- paste(cantidades$folio, cantidades$n_linea, sep = "_")

# =============================================================================
# FILTRO (OPCIÓN A): SERVICIOS + SOLO "RESTAURANTES Y BARES"
# CCIF:
#  - 11.1.1.01.02 → Otros destilados consumidos en restaurantes
#  - 11.1.1.01.08 → Papas fritas y tablas consumidas en restaurantes
#  - 11.1.1.01.09 → Cervezas con alcohol (si aplica)
# =============================================================================

ccif_servicios <- c("11.1.1.01.02", "11.1.1.01.08", "11.1.1.01.09")

cantidades_servicios <- subset(
  cantidades,
  (ccif %in% ccif_servicios) &
    macrozona == 2 &
    glosa_establecimiento == "RESTAURANTES Y BARES"
)

# --- SUMA GASTO TOTAL POR PERSONA ---
gasto_servicios_por_persona <- aggregate(gasto ~ id_persona, data = cantidades_servicios, sum)
names(gasto_servicios_por_persona)[2] <- "gasto_servicios"

# --- MERGE: Gasto con personas ---
personas_gs <- merge(personas_gs, gasto_servicios_por_persona, by = "id_persona", all.x = TRUE)
personas_gs$gasto_servicios[is.na(personas_gs$gasto_servicios)] <- 0

# =============================================================================
# SOLUCIÓN LIMPIA: ARMONIZAR 'sexo' ANTES DE ENTRENAR Y PREDECIR
# =============================================================================

personas_gs$sexo <- factor(
  as.character(personas_gs$sexo),
  levels = c("1", "2"),
  labels = c("Hombre", "Mujer")
)

# --- VARIABLE BINARIA DE GASTO ---
personas_gs$incurre_gasto <- ifelse(personas_gs$gasto_servicios > 0, 1, 0)

# --- AGRUPACIÓN ESCOLARIDAD ---
personas_gs$grupo_escolaridad <- cut(
  personas_gs$edue,
  breaks = c(-Inf, 12, 14, 16, Inf),
  labels = c("Escolar", "Tecnico", "Universitaria", "Postgrado"),
  right = TRUE
)

# --- BASE PARA MODELO CONTINUO (solo quienes gastan) ---
tabla_gasto <- subset(personas_gs, gasto_servicios > 0)
tabla_gasto <- tabla_gasto[, c("sexo", "edad", "edue", "ing_pc", "gasto_servicios", "grupo_escolaridad")]

# --- TRANSFORMACIONES DE VARIABLES ---
tabla_gasto$log_ing_pc <- log(tabla_gasto$ing_pc)
tabla_gasto$log_gasto_servicios <- log(tabla_gasto$gasto_servicios + 1)
tabla_gasto$rango_edad <- cut(
  tabla_gasto$edad,
  breaks = c(0, 29, 44, 64, Inf),
  labels = c("jovenes", "adultos_jovenes", "adultos", "adultos_mayores")
)

# --- FILTRO DE OUTLIERS (percentil 1 y 99) ---
q_ing <- quantile(tabla_gasto$ing_pc, probs = c(0.01, 0.99))
q_gasto <- quantile(tabla_gasto$gasto_servicios, probs = c(0.01, 0.99))

tabla_gasto <- subset(
  tabla_gasto,
  ing_pc >= q_ing[1] & ing_pc <= q_ing[2] &
    gasto_servicios >= q_gasto[1] & gasto_servicios <= q_gasto[2]
)

# =============================================================================
# --- GRAFICOS EXPLORATORIOS (EJES EN ESPAÑOL + NÚMEROS SIN NOTACIÓN CIENTÍFICA) ---
# =============================================================================

old_scipen <- getOption("scipen")
options(scipen = 999)

fmt_miles <- function(x) format(round(x, 0), big.mark = ".", decimal.mark = ",", scientific = FALSE)

# HISTOGRAMA INGRESO
h1 <- hist(tabla_gasto$ing_pc, breaks = 30, col = "lightblue",
           main = "Distribución del Ingreso",
           xlab = "Ingreso per cápita",
           ylab = "Frecuencia",
           xaxt = "n")
ticks1 <- pretty(h1$breaks)
axis(1, at = ticks1, labels = fmt_miles(ticks1))

# HISTOGRAMA GASTO
h2 <- hist(tabla_gasto$gasto_servicios, breaks = 30, col = "lightblue",
           main = "Distribución del Gasto (2 servicios, RyB)",
           xlab = "Gasto total en servicios",
           ylab = "Frecuencia",
           xaxt = "n")
ticks2 <- pretty(h2$breaks)
axis(1, at = ticks2, labels = fmt_miles(ticks2))

# BOXPLOT GASTO SEGÚN SEXO
boxplot(gasto_servicios ~ sexo, data = tabla_gasto,
        main = "Gasto (2 servicios, RyB) según Sexo",
        xlab = "Sexo",
        ylab = "Gasto",
        col = c("tomato", "lightgreen"))

# EDAD vs GASTO (sin notación científica en ejes)
plot(tabla_gasto$edad, tabla_gasto$gasto_servicios,
     main = "Edad vs Gasto (2 servicios, RyB)",
     xlab = "Edad",
     ylab = "Gasto",
     pch = 20, col = rgb(0, 0, 0, 0.3))
lines(lowess(tabla_gasto$edad, tabla_gasto$gasto_servicios), col = "red", lwd = 2)

# INGRESO vs GASTO (eje X formateado con puntos)
plot(tabla_gasto$ing_pc, tabla_gasto$gasto_servicios,
     main = "Ingreso vs Gasto (2 servicios, RyB)",
     xlab = "Ingreso per cápita",
     ylab = "Gasto",
     pch = 20, col = rgb(0, 0, 0, 0.3),
     xaxt = "n")
axis(1, at = pretty(tabla_gasto$ing_pc), labels = fmt_miles(pretty(tabla_gasto$ing_pc)))
lines(lowess(tabla_gasto$ing_pc, tabla_gasto$gasto_servicios), col = "blue", lwd = 2)

# BOXPLOT GASTO SEGÚN ESCOLARIDAD
boxplot(gasto_servicios ~ grupo_escolaridad, data = tabla_gasto,
        main = "Gasto (2 servicios, RyB) según Escolaridad",
        xlab = "Escolaridad",
        ylab = "Gasto",
        col = "skyblue")

options(scipen = old_scipen)

# -----------------------------------------------------------------------------
# MODELO LINEAL (CONTINUO)
# -----------------------------------------------------------------------------
modelo_lineal <- lm(
  log_gasto_servicios ~ grupo_escolaridad + ing_pc + rango_edad + sexo,
  data = tabla_gasto
)

summary(modelo_lineal)

# -----------------------------------------------------------------------------
# MODELO LOGIT (INCURRENCIA)
# -----------------------------------------------------------------------------
modelo_data <- subset(personas_gs,
                      !is.na(edad) & !is.na(grupo_escolaridad) & !is.na(sexo) & !is.na(ing_pc))

modelo_logit <- glm(
  incurre_gasto ~ sexo + edad + grupo_escolaridad + ing_pc,
  data = modelo_data,
  family = binomial
)

# --- PREDICCIONES DE PROBABILIDAD ---
modelo_data$prob_predicha <- predict(modelo_logit, type = "response")

# --- EVALUACIÓN CON UMBRAL 0.5 ---
modelo_data$clasificacion_05 <- ifelse(modelo_data$prob_predicha >= 0.5, 1, 0)

cat("---- Evaluación con umbral 0.5 ----\n")
conf_05 <- table(Real = modelo_data$incurre_gasto,
                 Predicha = modelo_data$clasificacion_05)
print(conf_05)

cat("Accuracy:", mean(modelo_data$incurre_gasto == modelo_data$clasificacion_05), "\n")

# --- CURVA ROC Y AUC ---
roc_obj <- roc(modelo_data$incurre_gasto, modelo_data$prob_predicha)
plot(roc_obj, col = "blue", main = "Curva ROC")
cat("AUC:", auc(roc_obj), "\n")

# --- UMBRAL ÓPTIMO (YOUDEN) ---
coords_opt <- coords(roc_obj, "best", ret = c("threshold", "sensitivity", "specificity"))
umbral_optimo <- as.numeric(coords_opt["threshold"])

cat("Umbral óptimo:", umbral_optimo, "\n")
cat("Sensibilidad óptima (Youden):", coords_opt["sensitivity"][[1]], "\n")
cat("Especificidad óptima (Youden):", coords_opt["specificity"][[1]], "\n")

# --- EVALUACIÓN CON UMBRAL ÓPTIMO ---
modelo_data$clasificacion_optima <- ifelse(modelo_data$prob_predicha >= umbral_optimo, 1, 0)

cat("\n---- Evaluación con umbral óptimo ----\n")
conf_opt <- table(Real = modelo_data$incurre_gasto,
                  Predicha = modelo_data$clasificacion_optima)
print(conf_opt)

accuracy_opt <- mean(modelo_data$incurre_gasto == modelo_data$clasificacion_optima)
cat("Accuracy (óptimo):", accuracy_opt, "\n")

#### CASEN ####

casen <- readRDS("D:/usach/2025-S2/geomarketing/rds/casen_base_preprocesado.rds")

casen$grupo_escolaridad <- cut(
  casen$esc,
  breaks = c(-Inf, 12, 14, 16, Inf),
  labels = c("Escolar", "Tecnico", "Universitaria", "Postgrado"),
  right = TRUE
)

casen$rango_edad <- cut(
  casen$edad,
  breaks = c(0, 29, 44, 64, Inf),
  labels = c("jovenes", "adultos_jovenes", "adultos", "adultos_mayores")
)

casen$ing_pc <- casen$ypc
casen <- casen[!is.na(casen$ing_pc), ]

casen$sexo <- factor(
  as.character(casen$sexo),
  levels = c("1", "2"),
  labels = c("Hombre", "Mujer")
)

# Predecir probabilidad de incurrir en gasto
casen$prob_predicha <- predict(modelo_logit, newdata = casen, type = "response")

# Clasificar según umbral óptimo
casen$clasificacion <- ifelse(casen$prob_predicha >= umbral_optimo, 1, 0)

# Filtrar quienes incurren en gasto
casen_pred <- casen[casen$clasificacion == 1, ]

# Predecir en escala log (modelo continuo)
casen_pred$log_gasto_estimado <- predict(modelo_lineal, newdata = casen_pred)

# Volver a escala natural (log(gasto + 1))
casen_pred$gasto_estimado <- exp(casen_pred$log_gasto_estimado) - 1

# Visualizamos el gasto que se predijo
plot(density(casen_pred$gasto_estimado), col = "blue", lwd = 2,
     main = "Gasto Predicho (servicios, RyB)")

# Controlar outliers (Winsorización)
casen_pred$gasto_estimado_wins <- pmin(casen_pred$gasto_estimado, quantile(casen_pred$gasto_estimado, 0.999))

# Comparaciones básicas
summary(tabla_gasto$gasto_servicios)
summary(casen_pred$gasto_estimado_wins)

sd(tabla_gasto$gasto_servicios)
sd(casen_pred$gasto_estimado_wins)

# Densidad: EPF vs CASEN imputado
plot(density(tabla_gasto$gasto_servicios), col = "blue", lwd = 2,
     main = "Densidad: EPF vs CASEN imputado (servicios, RyB)")
lines(density(casen_pred$gasto_estimado_wins), col = "red", lwd = 2)
legend("topright", legend = c("EPF", "CASEN imputado"), col = c("blue", "red"), lwd = 2)



# Selección de consumidores y control de outliers
tabla_gasto <- subset(personas_gs, gasto_servicios > 0)
q_ing   <- quantile(tabla_gasto$ing_pc, probs = c(0.01, 0.99))
q_gasto <- quantile(tabla_gasto$gasto_servicios, probs = c(0.01, 0.99))




