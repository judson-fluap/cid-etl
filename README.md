# ETL CID-10 (Fluap) — Guia técnico

Este projeto lê os CSV oficiais do DataSUS (CID10CSV), constrói a hierarquia de Capítulo/Bloco e consolida um único CSV pronto para uso em Excel.

## Arquivos
- etl_cid10.py
- requirements.txt
- Saída: `cid10_consolidado.csv` (padrão `;`, `utf-8-sig`, aspas em todas as células)

## Requisitos
- Windows 10/11
- Python 3.13+
- Pip

## Setup de ambiente
PowerShell (recomendado):

```powershell
# Na pasta do projeto
cd C:\Users\user\Desktop\fluap\etl

# (Opcional) ambiente virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Dependências
python -m pip install -r requirements.txt
```

Bash (WSL/Linux/macOS):

```bash
cd /mnt/c/Users/user/Desktop/fluap/etl
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

CMD (legado):

```bat
cd C:\Users\user\Desktop\fluap\etl
python -m venv .venv
call .venv\Scripts\activate.bat
python -m pip install -r requirements.txt
```

## Dados de entrada (oficial)
- Baixe e extraia o pacote CID10CSV do DataSUS em um diretório, ex.: `C:\Users\user\Downloads\CID10CSV`.
- O script procura arquivos contendo os fragmentos: `CAPITULOS`, `GRUPOS`, `CATEGORIAS`, `SUBCATEGORIAS` (CSV, `;`, `latin1`).

## Execução — modo recomendado (apenas DataSUS)
PowerShell:

```powershell
python etl_cid10.py --datasus_dir "C:\Users\user\Downloads\CID10CSV" --out "C:\Users\user\Desktop\fluap\etl\cid10_consolidado.csv"
```

Bash:

```bash
python etl_cid10.py --datasus_dir "/mnt/c/Users/user/Downloads/CID10CSV" --out "/mnt/c/Users/user/Desktop/fluap/etl/cid10_consolidado.csv"
```

Saída no terminal:
- Total de códigos consolidados (≈12.4k)
- Registros sem bloco/capítulo (idealmente 0)
- Caminho do arquivo exportado

## Execução — modo combinado (opcional)
Se tiver fonte estruturada (chapters.csv, blocks.csv, categories.csv, subcategories.csv) e um CSV do DATASUS com `codigo;descricao`:

PowerShell (quebra de linha com ^):

```powershell
python etl_cid10.py ^
  --datasus "C:\caminho\datasus.csv" ^
  --chapters "C:\caminho\chapters.csv" ^
  --blocks "C:\caminho\blocks.csv" ^
  --categories "C:\caminho\categories.csv" ^
  --subcategories "C:\caminho\subcategories.csv" ^
  --out "C:\Users\user\Desktop\fluap\etl\cid10_consolidado.csv"
```

## Modelo de dados (saída)
- cid_codigo (ex.: T65.9)
- cid_categoria (ex.: T65)
- cid_subcategoria (quando existir)
- titulo, descricao
- capitulo_codigo (faixa, ex.: A00-A09), capitulo_titulo
- bloco_codigo (faixa, ex.: T51-T65), bloco_titulo
- fonte (Estruturada ou DATASUS)
- dt_atualizacao