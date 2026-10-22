# ---------------------------------------------------------------------------
# 01_download.R  ->  Confere, ano a ano, a última atualização publicada no
# OpenDATASUS e baixa só os PARQUET que mudaram desde a última execução.
# Brutos NÃO entram no Git (ver .gitignore).
# ---------------------------------------------------------------------------
source("R/config.R")

dir.create(dir_raw, showWarnings = FALSE, recursive = TRUE)

message("Anos da série: ", paste(range(anos_srag), collapse = " a "))
fontes    <- resolver_urls_srag()
manifesto <- ler_manifesto()

if (nrow(fontes) == 0 && nrow(manifesto) == 0)
  stop("Não foi possível resolver nenhuma URL do SRAG.")

# Um ano precisa ser (re)processado quando:
#  - foi pedido SRAG_FORCAR, ou
#  - ainda não está no manifesto / o recorte do ano sumiu do repositório, ou
#  - o arquivo publicado tem outro nome (nova data de revisão), ou
#  - o site informa outra data de modificação para o mesmo nome.
precisa_atualizar <- function(i) {
  a <- fontes$ano[i]
  m <- manifesto[manifesto$ano == a, , drop = FALSE]
  if (forcar_tudo || nrow(m) == 0 || !file.exists(caminho_bruto(a))) return(TRUE)
  if (!identical(m$arquivo_fonte, fontes$arquivo_fonte[i])) return(TRUE)
  novo <- fontes$atualizado_fonte[i]; antigo <- m$atualizado_fonte
  !is.na(novo) && !is.na(antigo) && !identical(novo, antigo)
}
fontes$pendente <- vapply(seq_len(nrow(fontes)), precisa_atualizar, logical(1))

eh_parquet <- function(arq) {
  if (!file.exists(arq) || file.info(arq)$size < 12) return(FALSE)
  con <- file(arq, "rb"); on.exit(close(con))
  identical(rawToChar(readBin(con, "raw", 4)), "PAR1")
}

baixar <- function(url) {
  destino <- file.path(dir_raw, basename(sub("\\?.*$", "", url)))
  if (eh_parquet(destino)) {
    message("Já existe, pulando: ", destino)
    return(destino)
  }
  parcial <- paste0(destino, ".parcial")
  for (tentativa in 1:3) {
    message("Baixando: ", url, if (tentativa > 1) sprintf(" (tentativa %d)", tentativa) else "")
    ok <- tryCatch({
      h <- curl::new_handle(followlocation = TRUE, connecttimeout = 60, timeout = 3600,
                            failonerror = TRUE)
      curl::curl_download(url, parcial, handle = h, quiet = TRUE)
      eh_parquet(parcial)
    }, error = function(e) { message("  falhou: ", conditionMessage(e)); FALSE })
    if (ok) break
    unlink(parcial); Sys.sleep(10 * tentativa)
  }
  if (!ok) stop("Download falhou após 3 tentativas: ", url)
  file.rename(parcial, destino)
  message(sprintf("  ok: %.1f MB", file.info(destino)$size / 1024^2))
  destino
}

fontes$arquivo_local <- NA_character_
pend <- which(fontes$pendente)

# Vários arquivos (ex.: primeira carga da série): baixa todos em paralelo.
# O que falhar aqui é refeito abaixo, um a um, com novas tentativas.
destinos <- file.path(dir_raw, fontes$arquivo_fonte[pend])
falta    <- !vapply(destinos, eh_parquet, logical(1))
if (sum(falta) > 1 && utils::packageVersion("curl") >= "5.0.0") {
  message(sprintf("Baixando %d arquivos em paralelo...", sum(falta)))
  parciais <- paste0(destinos[falta], ".parcial")
  try(curl::multi_download(fontes$url[pend][falta], parciais, progress = FALSE,
                           timeout = 3600), silent = TRUE)
  for (j in seq_along(parciais)) {
    if (eh_parquet(parciais[j])) file.rename(parciais[j], destinos[falta][j]) else unlink(parciais[j])
  }
}
for (i in pend) fontes$arquivo_local[i] <- baixar(fontes$url[i])

saveRDS(fontes, file.path(dir_raw, "_manifest.rds"))

if (any(fontes$pendente)) {
  message("Download concluído. Anos a (re)processar: ",
          paste(fontes$ano[fontes$pendente], collapse = ", "))
} else {
  message("Nenhuma revisão nova no OpenDATASUS. Nada a baixar.")
}
