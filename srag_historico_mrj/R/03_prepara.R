#!/usr/bin/env Rscript
# =============================================================================
# 03_prepara.R  —  Preparo analítico da base SRAG / município do Rio de Janeiro
# -----------------------------------------------------------------------------
# Roda no GitHub Actions. Lê o recorte do município por ano
# (historico/bruto/*.parquet, gerados pelos passos 01/02) e aplica as
# derivações: vírus, faixa etária, território, atrasos, etc.
#
# Saídas:
#   historico/pronto/srag_mrj_pronto_<AAAA>.parquet  — série histórica, um
#       arquivo por ano. Só é refeito o ano cujo recorte mudou (ou todos,
#       quando este script muda), então os anos congelados não geram commit.
#   historico/serie_semanal.csv  — casos/óbitos/UTI por SE (2019 em diante)
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(lubridate)
  library(readr)
  library(duckdb)
  library(DBI)
})

source("R/config.R")   # caminhos da série histórica e manifesto

drv <- duckdb::duckdb()   # manter o driver numa variável (ver 02_filter_clean.R)
con <- dbConnect(drv)

# -----------------------------------------------------------------------------
# 1. TABELAS DE REFERÊNCIA
# -----------------------------------------------------------------------------
# Município do recorte. População: IBGE, Estimativas da População 2024
# (SIDRA, tabela 6579, variável 9324; Rio de Janeiro = 3304557).
# É um valor único, repetido em todos os anos (use denominadores do ano
# correspondente para incidência histórica).
mrj_ref <- tibble::tribble(
  ~codigo_municipio, ~municipio_ref,   ~uf_ref, ~populacao,
  330455,            "Rio de Janeiro", "RJ",    6729894
)
codigos_recorte <- mrj_ref$codigo_municipio

# --- Nomes de município -------------------------------------------------------
# O SRAG traz o nome em caixa alta e sem acento (ex.: "NOVA IGUACU"). Para os 92
# municípios do RJ usamos a tabela oficial do IBGE (R/municipios_rj.csv, código
# de 6 dígitos); para os demais, o nome da ficha com a caixa corrigida.
municipios_rj <- readr::read_csv("R/municipios_rj.csv", col_types = "cc", progress = FALSE)
nome_municipio_rj <- stats::setNames(municipios_rj$nome, municipios_rj$codigo)
nome_oficial <- function(codigo) {
  unname(nome_municipio_rj[as.character(as.integer(codigo))])
}
titulo_municipio <- function(x) {
  x <- str_to_title(normalizar_texto(x))
  str_replace_all(x, " (De|Da|Do|Das|Dos|E) ", function(m) str_to_lower(m))
}

# -----------------------------------------------------------------------------
# 2. HELPERS DE CONVERSÃO
# -----------------------------------------------------------------------------
normalizar_texto <- function(x) {
  x <- as.character(x); x <- str_squish(x); x[x == ""] <- NA_character_; x
}
converter_numero <- function(x) suppressWarnings(as.numeric(as.character(x)))

# Detecção viral vetorizada (código 1 = Sim). NA vira FALSE.
pos <- function(x) { v <- converter_numero(x); !is.na(v) & v == 1 }

converter_data <- function(x) {
  x <- as.character(x)
  res <- as.Date(rep(NA_real_, length(x)), origin = "1970-01-01")
  # 1) ISO com T e Z:  2026-04-04T00:00:00.000Z
  iso_tz <- suppressWarnings(as_date(as_datetime(x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")))
  f <- is.na(res); res[f] <- iso_tz[f]
  # 2) Data + hora com ESPAÇO (formato do parquet/DuckDB):  2026-05-04 00:00:00
  f <- is.na(res)
  if (any(f)) res[f] <- suppressWarnings(as_date(ymd_hms(x[f], quiet = TRUE)))
  # 3) ISO simples:  2026-05-04
  f <- is.na(res); if (any(f)) res[f] <- suppressWarnings(ymd(x[f], quiet = TRUE))
  # 4) Brasileiro:    04/05/2026
  f <- is.na(res); if (any(f)) res[f] <- suppressWarnings(dmy(x[f], quiet = TRUE))
  as.Date(res, origin = "1970-01-01")
}

classificar_unidade <- function(nome) {
  n <- str_to_upper(str_squish(as.character(nome)))
  dplyr::case_when(
    is.na(n) | n == "" ~ "Ignorada",
    str_detect(n, "\\bUPA\\b|\\bCER\\b|EMERGENCIA REGIONAL|EMERG.* REGIONAL|\\bAMIU\\b|\\bSEMIU\\b") ~ "UPA / Pronto-atendimento",
    str_detect(n, "\\bUBS\\b|\\bAPS\\b|\\bCF\\b|\\bCMS\\b|CLINICA DA FAMILIA|CENTRO MUNICIPAL DE SAUDE|ATENCAO PRIMARIA|SAUDE DA FAMILIA|POSTO.?DE.?SAUDE|UNID.*BASICA|POLICLINICA|\\bCAPS\\b|CENTRO.?DE.?SAUDE") ~ "APS / UBS",
    str_detect(n, "HOSP|MATERN|INSTITUTO|\\bINST\\b|\\bIFF\\b|\\bINI\\b|FIOCRUZ|CASA DE SAUDE|CLINICA|ONCO|UNIMED|SAMARITANO|BENEFICENCIA|CARDIAC|PEDIATRIC|\\bSANTA\\b|\\bSAO\\b") ~ "Hospital",
    str_detect(n, "PRONTO|URGENCIA") ~ "UPA / Pronto-atendimento",
    TRUE ~ "Outro / não classificado"
  )
}

# =============================================================================
# preparar_base(): seções 3 a 5 aplicadas a UM arquivo de entrada.
#   ENTRADA     recorte do município em parquet (gerado pelo 02_filter_clean.R)
#   id_inicial  deslocamento do id_caso. Na série histórica usamos ano * 1e6
#               (ex.: 2021000001) para o id não se repetir entre os anos.
# =============================================================================
# Colunas que as derivações realmente usam. O recorte tem 194; ler só estas
# (o parquet é colunar) corta a maior parte do tempo de leitura e de memória.
colunas_obrigatorias <- c("CO_MUN_NOT","CO_MUN_RES","SG_UF_NOT","SG_UF","DT_NOTIFIC",
                          "DT_SIN_PRI","SEM_PRI","SEM_NOT")
colunas_opcionais <- c(
  "NU_NOTIFIC","ID_MUNICIP","ID_MN_RESI","NM_UN_INTE","CS_SEXO","TP_IDADE","NU_IDADE_N",
  "CS_RACA","DT_INTERNA","DT_EVOLUCA","UTI","SUPORT_VEN","EVOLUCAO","CLASSI_FIN","PCR_RESUL",
  "RES_AN","ANTIVIRAL","VACINA","CARDIOPATI","DIABETES","OBESIDADE","IMUNODEPRE","RENAL",
  "NEUROLOGIC","PNEUMOPATI","ASMA","POS_PCRFLU","TP_FLU_PCR","POS_AN_FLU","TP_FLU_AN",
  "PCR_VSR","PCR_PARA1","PCR_PARA2","PCR_PARA3","PCR_PARA4","PCR_ADENO","PCR_METAP","PCR_BOCA",
  "PCR_RINO","PCR_SARS2","PCR_OUTRO","AN_SARS2","AN_VSR","AN_PARA1","AN_PARA2","AN_PARA3",
  "AN_ADENO","AN_OUTRO"
)

preparar_base <- function(ENTRADA, id_inicial = 0L) {
# -----------------------------------------------------------------------------
# 3. LEITURA (tudo como texto: as regras abaixo convertem cada campo)
# -----------------------------------------------------------------------------
cat("Lendo", ENTRADA, "...\n")
existentes <- dbGetQuery(con, sprintf(
  "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", ENTRADA))$column_name
faltando <- setdiff(colunas_obrigatorias, toupper(existentes))
if (length(faltando) > 0)
  stop("Colunas obrigatórias ausentes em ", ENTRADA, ": ", paste(faltando, collapse = ", "))
usar <- existentes[toupper(existentes) %in% c(colunas_obrigatorias, colunas_opcionais)]

base <- tibble::as_tibble(dbGetQuery(con, sprintf(
  "SELECT %s FROM read_parquet('%s')",
  paste(sprintf('CAST("%s" AS VARCHAR) AS "%s"', usar, toupper(usar)), collapse = ", "),
  ENTRADA)))
# Mesmo tratamento que a leitura por CSV dava: apara espaços e trata vazios como NA.
base[] <- lapply(base, function(x) {
  x <- trimws(x); x[x %in% c("", "NA", "N/A", "NULL")] <- NA_character_; x
})
cat("Linhas lidas:", nrow(base), "\n")

# Garante colunas opcionais ausentes
for (col in setdiff(colunas_opcionais, names(base))) base[[col]] <- NA_character_

# -----------------------------------------------------------------------------
# 4. DERIVAÇÕES
# -----------------------------------------------------------------------------
ref_residencia <- mrj_ref %>%
  transmute(CO_MUN_RES = codigo_municipio, populacao_residencia = populacao)

base <- base %>%
  mutate(
    CO_MUN_NOT = converter_numero(CO_MUN_NOT),
    CO_MUN_RES = converter_numero(CO_MUN_RES),
    SG_UF_NOT  = str_to_upper(normalizar_texto(SG_UF_NOT)),
    SG_UF      = str_to_upper(normalizar_texto(SG_UF)),
    ID_MUNICIP = normalizar_texto(ID_MUNICIP),
    ID_MN_RESI = normalizar_texto(ID_MN_RESI)
  ) %>%
  # Mantém o mesmo critério abrangente do 02_filter_clean.R (notificação OU
  # residência no município). O 02 já filtrou; este filtro é só uma salvaguarda
  # e NÃO deve excluir residentes do município notificados fora dele.
  filter(CO_MUN_NOT %in% codigos_recorte | CO_MUN_RES %in% codigos_recorte) %>%
  left_join(ref_residencia,  by = "CO_MUN_RES") %>%
  mutate(
    id_caso = as.integer(id_inicial) + row_number(),
    data_notificacao     = converter_data(DT_NOTIFIC),
    data_inicio_sintomas = converter_data(DT_SIN_PRI),
    data_internacao      = converter_data(DT_INTERNA),
    data_evolucao        = converter_data(DT_EVOLUCA),
    # Semana epidemiológica pelo calendário oficial (domingo a sábado; a SE 01
    # é a que tem mais dias em janeiro) -- é o que epiweek()/epiyear() calculam.
    # Calculada a partir da DATA dos primeiros sintomas, e não do campo SEM_PRI,
    # porque o SEM_PRI do Sivep diverge do calendário na virada 2025/2026:
    # marca 28/12/2025-03/01/2026 como SE 01 (o oficial é SE 53/2025) e deixa
    # 2026 inteiro uma semana adiantado (04-10/01/2026 sai SE 02; o oficial é
    # SE 01). (O isoyear() usado antes começa a semana na segunda e rotulava o
    # domingo da virada no ano errado, ex.: 29/12/2024 saía "2024-SE01".)
    # Semana e ano saem SEMPRE da mesma data de referência (sintomas e, na falta
    # dela, notificação), para os dois campos nunca usarem calendários diferentes.
    # SEM_PRI/SEM_NOT só entram quando o registro não tem nenhuma das duas datas;
    # nesse caso o ano fica vazio e o registro não entra na série semanal.
    data_ref_se = coalesce(data_inicio_sintomas, data_notificacao),
    semana_epidemiologica = coalesce(as.integer(epiweek(data_ref_se)),
                                     as.integer(converter_numero(SEM_PRI)),
                                     as.integer(converter_numero(SEM_NOT))),
    ano_epidemiologico = as.integer(epiyear(data_ref_se)),
    periodo_se = if_else(!is.na(ano_epidemiologico) & !is.na(semana_epidemiologica),
                         sprintf("%d-SE%02d", ano_epidemiologico, semana_epidemiologica),
                         NA_character_),
    atraso_notificacao = as.numeric(data_notificacao - data_inicio_sintomas),
    atraso_internacao  = as.numeric(data_internacao - data_inicio_sintomas),
    atraso_notificacao = if_else(atraso_notificacao >= 0 & atraso_notificacao <= 60, atraso_notificacao, NA_real_),
    atraso_internacao  = if_else(atraso_internacao  >= 0 & atraso_internacao  <= 60, atraso_internacao,  NA_real_),
    territorio_notificacao = case_when(
      CO_MUN_NOT %in% codigos_recorte ~ "Município do Rio de Janeiro",
      SG_UF_NOT == "RJ" ~ "Outros municípios do RJ",
      !is.na(SG_UF_NOT) ~ "Outras UFs",
      TRUE ~ "Não classificado"),
    municipio_notificacao = coalesce(nome_oficial(CO_MUN_NOT), titulo_municipio(ID_MUNICIP), "Município ignorado"),
    territorio_residencia = case_when(
      CO_MUN_RES %in% codigos_recorte ~ "Município do Rio de Janeiro",
      SG_UF == "RJ" ~ "Outros municípios do RJ",
      !is.na(SG_UF) ~ "Outras UFs",
      TRUE ~ "Residência ignorada"),
    municipio_residencia = coalesce(nome_oficial(CO_MUN_RES), titulo_municipio(ID_MN_RESI), "Município ignorado"),
    unidade_internacao = coalesce(normalizar_texto(str_to_title(NM_UN_INTE)), "Ignorada"),
    tipo_unidade = classificar_unidade(NM_UN_INTE),
    # ATENÇÃO: é uma INFERÊNCIA pelo nome da unidade (UPA, CER, pronto-atendimento),
    # não um campo da ficha. A base não informa se houve internação sem leito;
    # leia como "notificado em unidade de pronto-atendimento".
    internacao_sem_leito = tipo_unidade == "UPA / Pronto-atendimento",
    sexo = case_when(CS_SEXO %in% c("M","1") ~ "Masculino",
                     CS_SEXO %in% c("F","2") ~ "Feminino", TRUE ~ "Ignorado"),
    raca_cor = case_when(
      converter_numero(CS_RACA) == 1 ~ "Branca",
      converter_numero(CS_RACA) == 2 ~ "Preta",
      converter_numero(CS_RACA) == 3 ~ "Amarela",
      converter_numero(CS_RACA) == 4 ~ "Parda",
      converter_numero(CS_RACA) == 5 ~ "Indígena", TRUE ~ "Ignorada"),
    idade_anos = case_when(
      converter_numero(TP_IDADE) == 1 ~ converter_numero(NU_IDADE_N) / 365.25,
      converter_numero(TP_IDADE) == 2 ~ converter_numero(NU_IDADE_N) / 12,
      converter_numero(TP_IDADE) == 3 ~ converter_numero(NU_IDADE_N), TRUE ~ NA_real_),
    faixa_etaria = case_when(
      is.na(idade_anos) ~ "Ignorada",
      idade_anos < 1  ~ "< 1 ano",   idade_anos < 5  ~ "1–4 anos",
      idade_anos < 10 ~ "5–9 anos",  idade_anos < 20 ~ "10–19 anos",
      idade_anos < 40 ~ "20–39 anos",idade_anos < 60 ~ "40–59 anos",
      idade_anos < 80 ~ "60–79 anos",TRUE ~ "80 anos ou mais"),
    uti = case_when(converter_numero(UTI) == 1 ~ "Sim",
                    converter_numero(UTI) == 2 ~ "Não", TRUE ~ "Ignorado"),
    suporte_ventilatorio = case_when(
      converter_numero(SUPORT_VEN) == 1 ~ "Ventilação invasiva",
      converter_numero(SUPORT_VEN) == 2 ~ "Ventilação não invasiva",
      converter_numero(SUPORT_VEN) == 3 ~ "Não utilizou", TRUE ~ "Ignorado"),
    evolucao = case_when(
      converter_numero(EVOLUCAO) == 1 ~ "Cura",
      converter_numero(EVOLUCAO) == 2 ~ "Óbito",
      converter_numero(EVOLUCAO) == 3 ~ "Óbito por outras causas",
      TRUE ~ "Ignorado/em acompanhamento"),
    # Evolução 1 = cura, 2 = óbito (SRAG), 3 = óbito por outras causas, 9 ou vazio =
    # ignorado/em acompanhamento. Encerrado = qualquer desfecho conhecido (1, 2 ou 3).
    # obito_srag fica NA quando o desfecho é desconhecido (9 ou vazio), em vez de
    # FALSE, para que "ignorado" nunca seja contado como "não óbito".
    caso_encerrado = converter_numero(EVOLUCAO) %in% c(1, 2, 3),
    obito_srag = case_when(converter_numero(EVOLUCAO) == 2 ~ TRUE,
                           converter_numero(EVOLUCAO) %in% c(1, 3) ~ FALSE,
                           TRUE ~ NA),
    classificacao_final = case_when(
      converter_numero(CLASSI_FIN) == 1 ~ "SRAG por influenza",
      converter_numero(CLASSI_FIN) == 2 ~ "SRAG por outro vírus respiratório",
      converter_numero(CLASSI_FIN) == 3 ~ "SRAG por outro agente etiológico",
      converter_numero(CLASSI_FIN) == 4 ~ "SRAG não especificada",
      converter_numero(CLASSI_FIN) == 5 ~ "SRAG por COVID-19", TRUE ~ "Não classificada"),
    pcr_realizado  = converter_numero(PCR_RESUL) %in% c(1, 2, 3),
    pcr_detectavel = converter_numero(PCR_RESUL) == 1,
    an_realizado   = converter_numero(RES_AN) %in% c(1, 2, 3),
    an_positivo    = converter_numero(RES_AN) == 1,
    antiviral = case_when(converter_numero(ANTIVIRAL) == 1 ~ "Sim",
                          converter_numero(ANTIVIRAL) == 2 ~ "Não", TRUE ~ "Ignorado"),
    vacina_gripe = case_when(converter_numero(VACINA) == 1 ~ "Sim",
                             converter_numero(VACINA) == 2 ~ "Não", TRUE ~ "Ignorado"),
    possui_cardiopatia = converter_numero(CARDIOPATI) == 1,
    possui_diabetes    = converter_numero(DIABETES) == 1,
    possui_obesidade   = converter_numero(OBESIDADE) == 1,
    possui_imunodepre  = converter_numero(IMUNODEPRE) == 1,
    possui_renal       = converter_numero(RENAL) == 1,
    possui_pneumopatia = converter_numero(PNEUMOPATI) == 1,
    possui_neurologica = converter_numero(NEUROLOGIC) == 1,
    possui_asma        = converter_numero(ASMA) == 1
  )

# --- Detecção viral vetorizada ------------------------------------------------
tp_flu_pcr <- converter_numero(base$TP_FLU_PCR)
tp_flu_an  <- converter_numero(base$TP_FLU_AN)
flu_pcr    <- pos(base$POS_PCRFLU)
flu_an     <- pos(base$POS_AN_FLU)
ind <- cbind(
  "VSR"                     = pos(base$PCR_VSR)   | pos(base$AN_VSR),
  "Parainfluenza 1"         = pos(base$PCR_PARA1) | pos(base$AN_PARA1),
  "Parainfluenza 2"         = pos(base$PCR_PARA2) | pos(base$AN_PARA2),
  "Parainfluenza 3"         = pos(base$PCR_PARA3) | pos(base$AN_PARA3),
  "Parainfluenza 4"         = pos(base$PCR_PARA4),
  "Adenovírus"              = pos(base$PCR_ADENO) | pos(base$AN_ADENO),
  "Metapneumovírus"         = pos(base$PCR_METAP),
  "Bocavírus"               = pos(base$PCR_BOCA),
  "Rinovírus"               = pos(base$PCR_RINO),
  "SARS-CoV-2"              = pos(base$PCR_SARS2) | pos(base$AN_SARS2),
  "Influenza A"             = (flu_pcr & tp_flu_pcr %in% 1) | (flu_an & tp_flu_an %in% 1),
  "Influenza B"             = (flu_pcr & tp_flu_pcr %in% 2) | (flu_an & tp_flu_an %in% 2),
  "Influenza não subtipada" = (flu_pcr & !(tp_flu_pcr %in% c(1,2))) | (flu_an & !(tp_flu_an %in% c(1,2))),
  "Outro vírus respiratório"= pos(base$PCR_OUTRO) | pos(base$AN_OUTRO)
)
ind[is.na(ind)] <- FALSE
rotulos_virus <- colnames(ind)

base$numero_virus <- as.integer(rowSums(ind))
# virus_detectados: string separada por ", " (o app reconstrói a base longa com separate_rows)
base$virus_detectados <- vapply(seq_len(nrow(ind)), function(i) {
  v <- rotulos_virus[ind[i, ]]
  if (length(v) == 0) "Nenhum vírus identificado" else paste(v, collapse = ", ")
}, character(1))
base$codeteccao <- dplyr::case_when(
  base$numero_virus == 0 ~ "Sem vírus identificado",
  base$numero_virus == 1 ~ "Detecção única",
  base$numero_virus >= 2 ~ "Codetecção", TRUE ~ "Ignorado")
base$resultado_viral <- ifelse(base$numero_virus > 0, "Vírus identificado", "Sem vírus identificado")

# -----------------------------------------------------------------------------
# 5. SELEÇÃO DAS COLUNAS ANALÍTICAS E GRAVAÇÃO
# -----------------------------------------------------------------------------
pronto <- base %>%
  transmute(
    id_caso,
    data_notificacao, data_inicio_sintomas, data_internacao, data_evolucao,
    ano_epidemiologico, semana_epidemiologica, periodo_se,
    territorio_notificacao, municipio_notificacao,
    territorio_residencia, municipio_residencia,
    populacao_residencia,
    unidade_internacao, tipo_unidade, internacao_sem_leito,
    sexo, raca_cor, idade_anos, faixa_etaria,
    virus_detectados, numero_virus, codeteccao, resultado_viral,
    uti, suporte_ventilatorio, evolucao, obito_srag, caso_encerrado,
    classificacao_final,
    pcr_realizado, pcr_detectavel, an_realizado, an_positivo,
    antiviral, vacina_gripe,
    possui_cardiopatia, possui_diabetes, possui_obesidade, possui_imunodepre,
    possui_renal, possui_pneumopatia, possui_neurologica, possui_asma,
    atraso_notificacao, atraso_internacao
  )

cat("  linhas:", nrow(pronto), "| colunas:", ncol(pronto),
    "| com vírus identificado:", sum(pronto$numero_virus > 0), "\n")
# Registros sem ano ou semana epidemiológica não entram na série semanal.
n_sem_se <- sum(is.na(pronto$ano_epidemiologico) | is.na(pronto$semana_epidemiologica))
if (n_sem_se > 0)
  warning(sprintf("%s: %d registro(s) sem ano/semana epidemiológica (ficam fora da série semanal).",
                  basename(ENTRADA), n_sem_se), call. = FALSE)
pronto
}

# -----------------------------------------------------------------------------
# 6. SÉRIE HISTÓRICA: um parquet pronto por ano
# -----------------------------------------------------------------------------
# Um ano só é refeito quando o recorte dele mudou ou quando este script mudou
# (pronto_ref = data do recorte + assinatura das regras). Assim a execução
# semanal toca só os anos vivos e os arquivos dos anos congelados ficam
# intactos no Git.
manifesto <- ler_manifesto()
manifesto <- manifesto[file.exists(caminho_bruto(manifesto$ano)), , drop = FALSE]
anos <- manifesto$ano
if (length(anos) == 0)
  stop("Nenhum recorte em ", dir_bruto, ". Rode antes: Rscript run_pipeline.R")

# Assinatura das regras: o próprio script, sem depender de fim de linha (CRLF/LF).
regras <- local({
  tmp <- tempfile(); on.exit(unlink(tmp))
  writeLines(sub("\r$", "", readLines("R/03_prepara.R", warn = FALSE, encoding = "UTF-8")),
             tmp, useBytes = TRUE)
  substr(unname(tools::md5sum(tmp)), 1, 12)
})
ref_atual <- paste(manifesto$processado_em, regras, sep = "|")
refazer <- forcar_tudo | is.na(manifesto$pronto_ref) | manifesto$pronto_ref != ref_atual |
           !file.exists(caminho_pronto(anos))

dir.create(dir_pronto, showWarnings = FALSE, recursive = TRUE)

for (k in which(refazer)) {
  a <- anos[k]
  pronto <- preparar_base(caminho_bruto(a), id_inicial = a * 1000000L)

  temp <- paste0(caminho_pronto(a), ".tmp")
  duckdb::duckdb_register(con, "pronto_r", pronto)
  dbExecute(con, sprintf("COPY (SELECT * FROM pronto_r ORDER BY id_caso) TO '%s' (%s);",
                         temp, opcoes_parquet))
  duckdb::duckdb_unregister(con, "pronto_r")
  file.rename(temp, caminho_pronto(a))
  cat(sprintf("Gravado: %s (%.2f MB)\n", caminho_pronto(a),
              file.info(caminho_pronto(a))$size / 1024^2))

  manifesto$pronto_ref[k] <- ref_atual[k]
  completo <- ler_manifesto()
  completo$pronto_ref[match(a, completo$ano)] <- ref_atual[k]
  gravar_manifesto(completo)
}
if (!any(refazer)) cat("Bases prontas já estão em dia; nada a refazer.\n")

# -----------------------------------------------------------------------------
# 7. SÉRIE SEMANAL AGREGADA (arquivo pequeno, pronto para curvas históricas)
# -----------------------------------------------------------------------------
# Agregada pelo DuckDB direto sobre os parquet de todos os anos. Fica em CSV
# porque é pequena e pode ser lida por painéis HTML sem biblioteca extra.
if (any(refazer) || !file.exists(arquivo_serie)) {
  arquivos_sql <- paste0("['", paste(caminho_pronto(anos), collapse = "','"), "']")
  serie_semanal <- tibble::as_tibble(dbGetQuery(con, sprintf("
    SELECT ano_epidemiologico, semana_epidemiologica, periodo_se,
           territorio_residencia, classificacao_final,
           CAST(count(*) AS INTEGER)                               AS casos,
           CAST(count(*) FILTER (WHERE obito_srag) AS INTEGER)     AS obitos_srag,
           CAST(count(*) FILTER (WHERE uti = 'Sim') AS INTEGER)    AS internacoes_uti,
           CAST(count(*) FILTER (WHERE numero_virus > 0) AS INTEGER) AS virus_identificado
    FROM read_parquet(%s, union_by_name = true)
    WHERE ano_epidemiologico IS NOT NULL AND semana_epidemiologica IS NOT NULL
    GROUP BY ALL
    ORDER BY ano_epidemiologico, semana_epidemiologica,
             territorio_residencia, classificacao_final",
    arquivos_sql)))
  readr::write_excel_csv(serie_semanal, arquivo_serie, na = "")
  cat("Gravado:", arquivo_serie, "| linhas:", nrow(serie_semanal),
      "| casos:", sum(serie_semanal$casos), "\n")
  # Conferência: a série semanal deve conter todos os registros das bases prontas.
  total_pronto <- as.numeric(dbGetQuery(con, sprintf(
    "SELECT count(*) AS n FROM read_parquet(%s, union_by_name = true)", arquivos_sql))$n)
  if (sum(serie_semanal$casos) != total_pronto)
    warning(sprintf("Série semanal com %d casos, bases prontas com %d: %d registro(s) sem ano/semana ficaram de fora.",
                    sum(serie_semanal$casos), total_pronto, total_pronto - sum(serie_semanal$casos)),
            call. = FALSE)
}

dbDisconnect(con, shutdown = TRUE)
cat("Série histórica:", min(anos), "a", max(anos), "|",
    sum(manifesto$linhas_recorte), "registros.\n")
