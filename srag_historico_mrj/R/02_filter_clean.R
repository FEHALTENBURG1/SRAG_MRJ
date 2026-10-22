# ---------------------------------------------------------------------------
# 02_filter_clean.R  ->  Filtra o município do Rio direto sobre cada parquet com DuckDB
# e grava UM recorte por ano (data/historico/bruto/*.parquet), versionado.
#
# Parquet -> parquet: os tipos da fonte são preservados e o DuckDB só
# descomprime as colunas/linhas de que precisa. Cada ano é processado
# separadamente porque o tipo de algumas colunas muda entre os arquivos
# (ex.: EVOLUCAO e PCR_SARS2 ora são texto, ora número).
# Só os anos que o 01_download.R marcou como pendentes são refeitos.
# ---------------------------------------------------------------------------
source("R/config.R")

library(duckdb)
library(DBI)

fontes <- readRDS(file.path(dir_raw, "_manifest.rds"))
pendentes <- fontes[fontes$pendente & !is.na(fontes$arquivo_local), , drop = FALSE]

dir.create(dir_bruto, showWarnings = FALSE, recursive = TRUE)
manifesto <- ler_manifesto()

# IMPORTANTE: manter o driver numa variável para o garbage collector
# do R nao descarta-lo e derrubar a conexao ("Invalid connection").
drv <- duckdb::duckdb()
con <- dbConnect(drv)

# Códigos como lista SQL. Comparamos por TEXTO para evitar surpresas de tipo.
codigos_sql <- paste0("'", paste(codigos_recorte, collapse = "','"), "'")
fmt <- function(n) format(n, big.mark = ".", decimal.mark = ",")

for (i in seq_len(nrow(pendentes))) {
  a       <- pendentes$ano[i]
  parquet <- pendentes$arquivo_local[i]
  destino <- caminho_bruto(a)
  temp    <- paste0(destino, ".tmp")

  message(sprintf("Filtrando município do Rio: %d (%s)...", a, basename(parquet)))
  # Mesmo critério da main: notificação OU residência no município do recorte.
  n <- dbExecute(con, sprintf("
    COPY (
      SELECT *
      FROM read_parquet('%s')
      WHERE CAST(CO_MUN_NOT AS VARCHAR) IN (%s)
         OR CAST(CO_MUN_RES AS VARCHAR) IN (%s)
    ) TO '%s' (%s);
  ", parquet, codigos_sql, codigos_sql, temp, opcoes_parquet))

  # Travas: não sobrescrever um ano bom com um recorte vazio ou mutilado.
  anterior <- manifesto$linhas_recorte[manifesto$ano == a]
  if (n == 0) {
    unlink(temp)
    stop(sprintf("Ano %d: nenhum registro do município do Rio. O layout do arquivo mudou?", a))
  }
  if (!forcar_tudo && length(anterior) == 1 && !is.na(anterior) && n < 0.5 * anterior) {
    unlink(temp)
    stop(sprintf(paste0("Ano %d: %d linhas contra %d na revisão anterior. Confira a fonte ",
                        "e rode com SRAG_FORCAR=1 se a queda for legítima."), a, n, anterior))
  }
  file.rename(temp, destino)

  tam_mb <- file.info(destino)$size / 1024^2
  message(sprintf("  linhas: %s | %.1f MB -> %s", fmt(n), tam_mb, destino))
  if (tam_mb > limite_mb_github) {
    warning(sprintf("%s (%.1f MB) acima de %d MB. Considere Git LFS.",
                    destino, tam_mb, limite_mb_github))
  }

  manifesto <- manifesto[manifesto$ano != a, , drop = FALSE]
  manifesto <- rbind(manifesto, data.frame(
    ano = a, arquivo_fonte = pendentes$arquivo_fonte[i],
    data_revisao = pendentes$data_revisao[i],
    atualizado_fonte = pendentes$atualizado_fonte[i],
    url = pendentes$url[i], linhas_recorte = as.integer(n),
    processado_em = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    pronto_ref = NA_character_,   # o 03 preenche depois de refazer a base pronta
    stringsAsFactors = FALSE))
  gravar_manifesto(manifesto)   # grava a cada ano: se cair no meio, retoma daqui
}

dbDisconnect(con, shutdown = TRUE)

if (nrow(pendentes) == 0) {
  message("Nenhum ano pendente; recortes mantidos como estão.")
} else {
  message(sprintf("Recorte concluído: %d ano(s) | %s linhas na série toda.",
                  nrow(pendentes), fmt(sum(manifesto$linhas_recorte))))
}
