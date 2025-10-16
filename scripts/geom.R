install.packages("DBI")
install.packages("RPostgres")
install.packages ("sf")

## Librerias
library(DBI)
library(RPostgres)
library(sf)

## Packages

########################
##Configuración DB##
########################

db_host = "localhost"
db_port = 5432
db_name = "censo_v_2017"
db_user = "postgres"
db_password = "postgres"

# Establecer conexión usando RPostgres #
con = dbConnect(
  Postgres(),
  dbname   = db_name,
  host     = db_host,
  port     = db_port,
  user     = db_user,
  password = db_password
)

##Consulta SQL##

consulta_sql = '
SELECT z.geocodigo,
    c.nom_comuna, 
    COUNT (*) FILTER (WHERE p.p15 >= 12 AND p.p15 <=14) AS total_profesionales,
    ROUND(COUNT (*) FILTER (WHERE p.p15 >= 12 AND p.p15 <=14) * 100.0/ COUNT(*)FILTER (WHERE p.p09 > 18) ,2) AS tasa_profesionales
FROM personas p
JOIN hogares h ON h.hogar_ref_id = p.hogar_ref_id 
JOIN viviendas v ON h.vivienda_ref_id = v.vivienda_ref_id 
JOIN zonas z ON z.zonaloc_ref_id = v.zonaloc_ref_id
JOIN comunas c ON z.codigo_comuna = c.codigo_comuna 
GROUP BY z.geocodigo, c.nom_comuna
ORDER BY tasa_profesionales DESC;
'

#Ejecutar la consulta
df_profesionales = dbGetQuery(con, consulta_sql)

#Cargar zonas censales y comunas
zonas_prof = st_read(con, query = "SELECT * FROM output.tasa_profesionales_geom;")
comunas = st_read(con, query = "SELECT * FROM dpa.comunas_v")

#histograma

ggplot(zonas_prof, aes(x = tasa_profesionales)) *
  geom_histogram(
    bins = 20,
    fill = "steelblue",
    alpha = 0.8
  ) +
  labs(
    title = "Distribución de tasa de profesionales", 
    x = "% Profesionales", 
    y "Frecuencia"
  )