# ---------------------------------------------------------------------------
# ler_historico.R  ->  Atalho para carregar a série histórica já pronta.
#
#   source("R/ler_historico.R")
#   srag <- ler_srag_mrj()                   # 2019 em diante
#   srag <- ler_srag_mrj(anos = 2023:2026)   # só alguns anos
#   srag <- ler_srag_mrj(colunas = c("periodo_se", "territorio_residencia",
#                                    "classificacao_final", "obito_srag"))
#
# Fora do repositório (ex.: no app publicado), leia direto do GitHub:
#   srag <- ler_srag_mrj(origem = paste0(
#     "https://raw.githubusercontent.com/FEHALTENBURG1/srag_historico/mrj/",
#     "historico"))
#
# Os arquivos são parquet: as datas já vêm como Date, os indicadores como
# lógico e os números como número, iguais em todos os anos. Quem preferir
# pode abrir a pasta direto com arrow::open_dataset("data/historico/pronto").
# ---------------------------------------------------------------------------
ler_srag_mrj <- function(anos = NULL, colunas = NULL,
                          origem = if (dir.exists("historico") && !dir.exists("data")) "historico"
                                   else "data/historico") {
  remoto <- grepl("^https?://", origem)
  junta  <- function(...) if (remoto) paste(..., sep = "/") else file.path(...)

  manifesto <- utils::read.csv(junta(origem, "manifesto.csv"), stringsAsFactors = FALSE)
  if (is.null(anos)) anos <- manifesto$ano
  anos <- sort(intersect(anos, manifesto$ano))
  if (length(anos) == 0) stop("Nenhum dos anos pedidos está na série.")
  arquivos <- junta(origem, "pronto", sprintf("srag_mrj_pronto_%d.parquet", anos))

  if (remoto) {   # baixa para uma pasta temporária (a série toda)
    locais <- file.path(tempdir(), basename(arquivos))
    for (i in seq_along(arquivos)) curl::curl_download(arquivos[i], locais[i], quiet = TRUE)
    arquivos <- locais
  }

  drv <- duckdb::duckdb(); con <- DBI::dbConnect(drv)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  campos <- if (is.null(colunas)) "*" else paste(sprintf('"%s"', colunas), collapse = ", ")
  tibble::as_tibble(DBI::dbGetQuery(con, sprintf(
    "SELECT %s FROM read_parquet([%s], union_by_name = true) ORDER BY id_caso",
    campos, paste(sprintf("'%s'", arquivos), collapse = ", "))))
}
