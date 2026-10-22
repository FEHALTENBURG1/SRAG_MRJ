# Orquestra o pipeline completo. Rode com: Rscript run_pipeline.R
#
#   01  confere a última atualização no OpenDATASUS e baixa o que mudou
#   02  filtra o município do Rio e grava um parquet por ano em historico/bruto/
#   03  (à parte) Rscript R/03_prepara.R  -> bases prontas + série semanal
#
# Exemplos:
#   Rscript run_pipeline.R                         # série toda, só o que mudou
#   SRAG_ANOS=2025,2026 Rscript run_pipeline.R     # só alguns anos
#   SRAG_FORCAR=1 Rscript run_pipeline.R           # refaz tudo do zero
source("R/01_download.R")
source("R/02_filter_clean.R")
message("Pipeline concluído. Recortes em ", dir_bruto, ".")
