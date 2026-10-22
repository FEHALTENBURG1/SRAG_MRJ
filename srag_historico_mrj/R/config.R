# ---------------------------------------------------------------------------
# config.R  ->  Parâmetros centrais do pipeline
# ---------------------------------------------------------------------------

# Recorte territorial DESTE branch: município do Rio de Janeiro (MRJ).
# Código IBGE de 6 dígitos, sem o dígito verificador (formato do SRAG:
# CO_MUN_NOT e CO_MUN_RES). A referência (nome, UF, população) fica em
# R/03_prepara.R. Para outro município, troque o código aqui e lá.
codigos_recorte <- 330455L   # Rio de Janeiro (RJ)

# Prefixo dos arquivos gerados: <prefixo>_bruto_AAAA.parquet etc.
prefixo_arquivo <- "srag_mrj"

# ---------------------------------------------------------------------------
# Série histórica: quais anos puxar.
# O OpenDATASUS publica UM arquivo por ano epidemiológico:
#   .../SRAG/<AAAA>/INFLUD<AA>-<DD-MM-AAAA>.parquet   (a data é a da revisão)
# Anos antigos ficam congelados por meses; o ano corrente e o anterior
# costumam ser republicados toda semana. Por isso o pipeline compara, ano a
# ano, a revisão publicada no site com a que já está no repositório
# (data/historico/manifesto.csv) e só baixa o que mudou.
#
# Variáveis de ambiente (todas opcionais):
#   SRAG_ANO_INICIAL  primeiro ano da série (padrão 2019)
#   SRAG_ANOS         lista explícita, ex.: "2025,2026" (sobrepõe o intervalo)
#   SRAG_URLS         URLs fixas separadas por vírgula (sobrepõe a descoberta)
#   SRAG_JANELA_DIAS  quantos dias para trás sondar no S3 se o site falhar (35)
#   SRAG_FORCAR       "1"/"true" para rebaixar e refiltrar todos os anos
# ---------------------------------------------------------------------------
ano_inicial <- as.integer(Sys.getenv("SRAG_ANO_INICIAL", "2019"))
ano_atual   <- as.integer(format(Sys.Date(), "%Y"))

anos_srag <- local({
  env <- trimws(Sys.getenv("SRAG_ANOS", ""))
  if (nzchar(env)) {
    sort(unique(as.integer(strsplit(env, "[,; ]+")[[1]])))
  } else {
    seq.int(ano_inicial, ano_atual)
  }
})

forcar_tudo <- tolower(Sys.getenv("SRAG_FORCAR", "")) %in% c("1", "true", "sim", "yes")
janela_dias <- as.integer(Sys.getenv("SRAG_JANELA_DIAS", "35"))

# Página do conjunto de dados: é ela que diz qual é a última atualização.
url_dataset <- "https://dadosabertos.saude.gov.br/dataset/srag-2019-a-2026"
s3_base     <- "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG"

# Última revisão conhecida de cada ano (conferida no site em 05/10/2026).
# Só é usada como último recurso, quando o site e a sondagem falham e o ano
# ainda não está no manifesto -- tipicamente na primeira carga da série.
revisoes_conhecidas <- c(
  "2019" = "23-03-2026", "2020" = "23-03-2026", "2021" = "23-03-2026",
  "2022" = "23-03-2026", "2023" = "23-03-2026", "2024" = "23-03-2026",
  "2025" = "28-09-2026", "2026" = "28-09-2026"
)

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0 || identical(a, "")) b else a

# ---- Helpers de nome de arquivo ---------------------------------------------
# Monta a URL do parquet de um ano para uma data de revisão (DD-MM-AAAA).
.url_srag <- function(ano, data_rev) {
  sprintf("%s/%d/INFLUD%02d-%s.parquet", s3_base, as.integer(ano),
          as.integer(ano) %% 100L, data_rev)
}

# Extrai a data de revisão (DD-MM-AAAA) do nome do arquivo.
.data_do_url <- function(u) {
  m <- regmatches(u, regexpr("\\d{2}-\\d{2}-\\d{4}", u))
  if (length(m) == 0 || m == "") return(as.Date(NA))
  as.Date(m, format = "%d-%m-%Y")
}

# Extrai o ano (AAAA) do nome do arquivo: INFLUD26-... -> 2026.
.ano_do_url <- function(u) {
  m <- regmatches(u, regexpr("INFLUD\\d{2}", u, ignore.case = TRUE))
  if (length(m) == 0 || m == "") return(NA_integer_)
  2000L + as.integer(sub("^INFLUD", "", toupper(m)))
}

# De uma tabela de candidatos (url, atualizado_fonte), fica com a revisão
# mais recente de cada ano.
.mais_recente_por_ano <- function(urls, atualizado = rep(NA_character_, length(urls))) {
  vazio <- data.frame(ano = integer(0), url = character(0),
                      atualizado_fonte = character(0), stringsAsFactors = FALSE)
  ok <- !is.na(urls) & nzchar(urls)
  urls <- urls[ok]; atualizado <- atualizado[ok]
  if (length(urls) == 0) return(vazio)
  anos  <- vapply(urls, .ano_do_url, integer(1), USE.NAMES = FALSE)
  datas <- do.call(c, lapply(urls, .data_do_url))
  tab <- data.frame(ano = anos, url = urls, atualizado_fonte = atualizado,
                    data = datas, stringsAsFactors = FALSE)
  tab <- tab[!is.na(tab$ano), , drop = FALSE]
  if (nrow(tab) == 0) return(vazio)
  tab <- tab[order(tab$ano, tab$data, tab$atualizado_fonte,
                   decreasing = TRUE, na.last = TRUE), , drop = FALSE]
  tab <- tab[!duplicated(tab$ano), c("ano", "url", "atualizado_fonte"), drop = FALSE]
  tab <- tab[order(tab$ano), , drop = FALSE]
  rownames(tab) <- NULL
  tab
}

# GET com novas tentativas (o portal às vezes derruba a conexão).
.baixar_texto <- function(u, tentativas = 4) {
  ultimo_erro <- NULL
  for (i in seq_len(tentativas)) {
    r <- tryCatch({
      h <- curl::new_handle(followlocation = TRUE, connecttimeout = 30, timeout = 90,
                            useragent = "Mozilla/5.0 (pipeline srag_historico, branch mrj; R curl)")
      resp <- curl::curl_fetch_memory(u, handle = h)
      if (resp$status_code != 200L) stop("HTTP ", resp$status_code)
      txt <- rawToChar(resp$content); Encoding(txt) <- "UTF-8"; txt
    }, error = function(e) e)
    if (!inherits(r, "error")) return(r)
    ultimo_erro <- r
    if (i < tentativas) Sys.sleep(5 * i)
  }
  stop(conditionMessage(ultimo_erro))
}

# O arquivo existe no S3? (HEAD; o bucket devolve 403 para o que não existe.)
.url_existe <- function(u) {
  tryCatch({
    h <- curl::new_handle(nobody = TRUE, connecttimeout = 20, timeout = 40)
    curl::curl_fetch_memory(u, handle = h)$status_code == 200L
  }, error = function(e) FALSE)
}

# ---- Fontes de descoberta ---------------------------------------------------
# (a) Página do dataset no OpenDATASUS. O HTML traz embutido (bloco
#     __NEXT_DATA__) o cadastro de cada recurso: nome ("2026- Banco vivo
#     28/09/2026 - PARQUET"), URL do arquivo e `last_modified`. É a mesma
#     informação que aparece na tela como data da última atualização.
.interpretar_pagina <- function(html) {
  urls <- character(0); atualizado <- character(0)

  bloco <- regmatches(html, regexpr(
    '(?s)<script id="__NEXT_DATA__"[^>]*>.*?</script>', html, perl = TRUE))
  if (length(bloco) == 1) {
    json <- sub("</script>$", "", sub("^<script[^>]*>", "", bloco))
    dados <- tryCatch(jsonlite::fromJSON(json, simplifyVector = FALSE),
                      error = function(e) NULL)
    recursos <- dados$props$pageProps$resources
    for (r in recursos) {
      u <- r$url %||% ""
      if (tolower(r$format %||% "") == "parquet" || grepl("\\.parquet$", tolower(u))) {
        urls       <- c(urls, u)
        atualizado <- c(atualizado, r$last_modified %||% r$metadata_modified %||% NA_character_)
      }
    }
  }
  # Reserva: se o formato do bloco mudar, pega os links .parquet crus do HTML.
  if (length(urls) == 0) {
    urls <- unique(unlist(regmatches(html, gregexpr(
      "https://[^\"'\\\\ <>]*INFLUD\\d{2}-\\d{2}-\\d{2}-\\d{4}\\.parquet", html))))
    atualizado <- rep(NA_character_, length(urls))
  }
  .mais_recente_por_ano(urls, atualizado)
}

.recursos_do_site <- function() {
  tryCatch({
    tab <- .interpretar_pagina(.baixar_texto(url_dataset))
    if (nrow(tab) == 0) stop("nenhum recurso .parquet encontrado na página")
    tab
  }, error = function(e) {
    message("  Site do OpenDATASUS indisponível (", conditionMessage(e),
            "); usando sondagem no S3.")
    .mais_recente_por_ano(character(0))
  })
}

# (b) Sondagem no S3: testa as datas de revisão dos últimos `janela_dias`
#     dias, da mais nova para a mais antiga. Substitui o antigo "plano B",
#     que só tentava a segunda-feira da semana e falhava quando o Ministério
#     ainda não tinha publicado o arquivo no momento da execução.
.url_por_sondagem <- function(ano, ate = as.Date(NA)) {
  dias <- Sys.Date() - seq.int(0, janela_dias)
  if (!is.na(ate)) dias <- dias[dias > ate]      # só o que for mais novo
  for (d in format(dias, "%d-%m-%Y")) {
    u <- .url_srag(ano, d)
    if (.url_existe(u)) return(u)
  }
  NA_character_
}

# ---- Manifesto (versionado): o que já está processado no repositório --------
# `pronto_ref` é preenchido pelo 03_prepara.R: identifica de qual recorte e
# com qual versão das regras a base pronta do ano foi gerada.
.colunas_manifesto <- c("ano", "arquivo_fonte", "data_revisao", "atualizado_fonte",
                        "url", "linhas_recorte", "processado_em", "pronto_ref")

ler_manifesto <- function() {
  if (!file.exists(arquivo_manifesto)) {
    return(data.frame(ano = integer(0), arquivo_fonte = character(0),
                      data_revisao = character(0), atualizado_fonte = character(0),
                      url = character(0), linhas_recorte = integer(0),
                      processado_em = character(0), pronto_ref = character(0),
                      stringsAsFactors = FALSE))
  }
  m <- utils::read.csv(arquivo_manifesto, stringsAsFactors = FALSE,
                       colClasses = "character", na.strings = c("", "NA"))
  for (col in setdiff(.colunas_manifesto, names(m))) m[[col]] <- NA_character_
  m$ano         <- as.integer(m$ano)
  m$linhas_recorte <- as.integer(m$linhas_recorte)
  m <- m[order(m$ano), .colunas_manifesto, drop = FALSE]
  rownames(m) <- NULL
  m
}

gravar_manifesto <- function(m) {
  dir.create(dirname(arquivo_manifesto), showWarnings = FALSE, recursive = TRUE)
  m <- m[order(m$ano), .colunas_manifesto, drop = FALSE]
  utils::write.csv(m, arquivo_manifesto, row.names = FALSE, na = "", fileEncoding = "UTF-8")
}

# ---- Resolução final --------------------------------------------------------
# Devolve um data.frame com uma linha por ano:
#   ano, url, arquivo_fonte, data_revisao, atualizado_fonte, origem
# Prioridade por ano:
#   1) SRAG_URLS (fixadas manualmente)
#   2) página do dataset no OpenDATASUS (data da última atualização no site)
#   3) sondagem no S3 por uma revisão mais nova que a do manifesto
#   4) a URL já registrada no manifesto (nada novo publicado)
#   5) última revisão conhecida em `revisoes_conhecidas`
resolver_urls_srag <- function(anos = anos_srag) {
  manifesto <- ler_manifesto()

  env <- Sys.getenv("SRAG_URLS", "")
  fixas <- .mais_recente_por_ano(if (nzchar(env)) trimws(strsplit(env, ",")[[1]]) else character(0))
  if (nrow(fixas) > 0) {
    message("Usando SRAG_URLS do ambiente.")
    if (!nzchar(Sys.getenv("SRAG_ANOS", ""))) anos <- fixas$ano   # só os anos informados
  }
  site <- if (nrow(fixas) == 0) .recursos_do_site() else .mais_recente_por_ano(character(0))

  linhas <- lapply(anos, function(a) {
    conhecido      <- manifesto$url[manifesto$ano == a]
    data_conhecida <- if (length(conhecido)) .data_do_url(conhecido) else as.Date(NA)

    u <- NA_character_; origem <- NA_character_; atualizado <- NA_character_
    if (a %in% fixas$ano) {
      u <- fixas$url[fixas$ano == a]; origem <- "SRAG_URLS"
    } else if (a %in% site$ano) {
      u <- site$url[site$ano == a]; origem <- "site OpenDATASUS"
      atualizado <- site$atualizado_fonte[site$ano == a]
    } else {
      u <- .url_por_sondagem(a, ate = data_conhecida)
      if (!is.na(u)) {
        origem <- "sondagem S3"
      } else if (length(conhecido)) {
        u <- conhecido; origem <- "manifesto (sem revisão nova)"
        atualizado <- manifesto$atualizado_fonte[manifesto$ano == a]
      } else if (as.character(a) %in% names(revisoes_conhecidas)) {
        cand <- .url_srag(a, revisoes_conhecidas[[as.character(a)]])
        if (.url_existe(cand)) { u <- cand; origem <- "revisão conhecida" }
      }
    }
    if (is.na(u)) {
      message(sprintf("  %d: nenhum arquivo encontrado (ainda não publicado?).", a))
      return(NULL)
    }
    message(sprintf("  %d: %s  [%s]", a, basename(u), origem))
    data.frame(ano = a, url = u, arquivo_fonte = basename(sub("\\?.*$", "", u)),
               data_revisao = format(.data_do_url(u), "%Y-%m-%d"),
               atualizado_fonte = atualizado, origem = origem,
               stringsAsFactors = FALSE)
  })
  out <- do.call(rbind, linhas)
  if (is.null(out)) {
    out <- data.frame(ano = integer(0), url = character(0), arquivo_fonte = character(0),
                      data_revisao = character(0), atualizado_fonte = character(0),
                      origem = character(0), stringsAsFactors = FALSE)
  }
  out
}

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------
dir_raw    <- "data-raw"
# Pasta de saída. O pipeline aceita os dois arranjos de repositório:
#   data/historico/...  (padrão)      ou      historico/... direto na raiz.
# Se já existir "historico" na raiz e não existir "data", grava na raiz.
# Para fixar, defina a variável de ambiente SRAG_DIR_SAIDA ("data" ou ".").
dir_out <- local({
  env <- Sys.getenv("SRAG_DIR_SAIDA", "")
  if (nzchar(env)) env
  else if (dir.exists("historico") && !dir.exists("data")) "."
  else "data"
})
dir_hist   <- file.path(dir_out, "historico")
dir_bruto  <- file.path(dir_hist, "bruto")    # recorte do MRJ, 1 parquet por ano
dir_pronto <- file.path(dir_hist, "pronto")   # base analítica, 1 parquet por ano

arquivo_manifesto <- file.path(dir_hist, "manifesto.csv")
arquivo_serie     <- file.path(dir_hist, "serie_semanal.csv")

caminho_bruto  <- function(ano) file.path(dir_bruto,  sprintf("%s_bruto_%d.parquet",  prefixo_arquivo, as.integer(ano)))
caminho_pronto <- function(ano) file.path(dir_pronto, sprintf("%s_pronto_%d.parquet", prefixo_arquivo, as.integer(ano)))

# Parquet com ZSTD: a série pronta inteira cabe em ~3 MB (eram ~64 MB em CSV)
# e é lida em uma fração de segundo, já com os tipos certos.
opcoes_parquet <- "FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9"

# Limite de segurança para commit no GitHub (MB) -- vale por arquivo.
limite_mb_github <- 90
