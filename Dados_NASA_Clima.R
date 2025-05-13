###############################################################################
#  COLETA DIÁRIA POWER NASA   –   AGREGAÇÃO ANUAL PARA >2 000 MUNICÍPIOS
#  Autor: Thaís Gama | Twanny Oliveira – rev. 11-mai-2025
###############################################################################

# Origem dos dados, veja: https://power.larc.nasa.gov/docs/methodology/data/processing/


## 1. Pacotes -----------------------------------------------------------------
# Instalar só se ainda não estiverem presentes
pkgs <- c("nasapower", "dplyr", "purrr", "progress")
new  <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
if(length(new)) install.packages(new)

# (opcional) ratelimitr requer Rtools; use Sys.sleep() se não quiser compilar
use_rl <- requireNamespace("ratelimitr", quietly = TRUE)
if(use_rl) library(ratelimitr)

library(nasapower)
library(dplyr)
library(purrr)
library(progress)

## 2. Dados de entrada --------------------------------------------------------
# Caminhos
dir_base <- "C:/Users/twanny.oliveira/Desktop/TESE - Novas Investigações/Dados Climáticos"

mun_all <- read.csv(file.path(dir_base, "municipios_latlong.txt"), sep = ";") |>
  mutate(GEOCODIGO_MUNICIPIO = sprintf("%07d", as.integer(GEOCODIGO_MUNICIPIO)))

cod_sel <- read.table(file.path(dir_base, "COD_MUN.txt"), sep = ";", header = TRUE,
                      colClasses = "character") |>
  mutate(COD_MUN = sprintf("%07d", as.integer(COD_MUN)))

#agora os tipos batem → semi_join funciona
mun_df <- semi_join(
  mun_all,
  cod_sel,
  by = c("GEOCODIGO_MUNICIPIO" = "COD_MUN")
)

## 3. Funções auxiliares ------------------------------------------------------
# 3.1 – download diário de um ano
get_year <- function(lat, lon, yr) {
  get_power(
    community     = "ag",
    lonlat        = c(lon, lat),
    pars          = c("RH2M", "T2M", "PRECTOTCORR"),
    temporal_api  = "daily",
    dates         = c(sprintf("%d-01-01", yr), sprintf("%d-12-31", yr))
  )
}

# 3.2 – aplicação de limite de taxa gentil
if(use_rl) {
  safe_get <- ratelimitr::limit_rate(
    get_year,
    ratelimitr::rate(n = 100, period = 60)      # 100 req/min
  )
} else {
  safe_get <- function(...) { Sys.sleep(0.6); tryCatch(get_year(...), error = \(e) NULL) }
}

fetch_point <- function(lat, lon, cod, nome) {
  anos <- 2013:2017
  pb   <- progress_bar$new(total = length(anos),
                           format = paste(nome, " [:bar] :current/:total :percent :eta"))
  map_dfr(anos, \(yy) {
    pb$tick()
    res <- safe_get(lat, lon, yy)
    if (is.null(res) || !nrow(res)) return(NULL)
    res$Municipio           <- nome
    res$GEOCODIGO_MUNICIPIO <- cod
    res
  })
}


# 4. Loop principal -----------------------------------------------------------
mun_sub <- mun_df |>
  transmute(
    lat  = as.numeric(LATITUDE),
    lon  = as.numeric(LONGITUDE),
    cod  = GEOCODIGO_MUNICIPIO,
    nome = NOME_MUNICIPIO
  )

safe_get <- function(lat, lon, yr, max_try = 5) {
  wait <- 10  # segundos
  for (i in seq_len(max_try)) {
    out <- tryCatch(
      get_year(lat, lon, yr),
      error = function(e) e
    )
    
    # sucesso → retorna
    if (!inherits(out, "error")) return(out)
    
    # Status HTTP dentro da mensagem (ex.: "... 502 ...")
    msg <- conditionMessage(out)
    if (grepl("\\b50[234]\\b", msg)) {
      message("⇢ Tentativa ", i, " falhou (", msg, "). Aguardando ", wait, " s…")
      Sys.sleep(wait)
      wait <- wait * 2        # back-off
    } else {
      stop(out)               # erro diferente – interrompe
    }
  }
  warning("Falha após ", max_try, " tentativas em ", lat, ", ", lon, ", ano ", yr)
  NULL                        # devolve NULL p/ fetch_point tratar
}

all_daily <- pmap_dfr(mun_sub, fetch_point)

## 5. Agregação anual ---------------------------------------------------------
dados_anuais <- all_daily %>%
  group_by(GEOCODIGO_MUNICIPIO, YEAR) %>%
  summarise(
    T2M_max      = max(T2M,  na.rm = TRUE),   # Temperatura máxima a 2m
    T2M_min      = min(T2M,  na.rm = TRUE),   # Temperatura mínima a 2m
    T2M_avg      = mean(T2M, na.rm = TRUE),   # Temperatura média a 2 m
    RH2M_max     = max(RH2M, na.rm = TRUE),   # Umidade relativa máxima a 2m
    RH2M_min     = min(RH2M, na.rm = TRUE),   # Umidade relativa mínima a 2m 
    RH2M_avg     = mean(RH2M, na.rm = TRUE),  # Umidade relativa média a 2m
    PRECTOT_sum  = sum(PRECTOTCORR, na.rm = TRUE), # Somatório anual de precipitação
    PRECTOT_avg  = mean(PRECTOTCORR, na.rm = TRUE), # Média anual de precipitação
    .groups = "drop"
  )

library(writexl)

# Salvar os dados em uma planilha do Excel
write_xlsx(dados_anuais, path = file.path(dir_base, "dados_climaticos_anuais.xlsx"))

head(dados_anuais)

cat("✓ Concluído! Dados salvos em", file.path(dir_base, "dados_climaticos_anuais.xlsx"), "\n")


           