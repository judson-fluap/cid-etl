import argparse
import os
from datetime import datetime
import pandas as pd
import csv

# =====================================
# Utilitários
# =====================================

def normalize_code(code: str) -> str:
    if pd.isna(code):
        return None
    return str(code).strip().upper()


def extract_root_category(code: str) -> str:
    if code is None:
        return None
    code = normalize_code(code)
    return code.split(".")[0]


# =====================================
# Leitura robusta
# =====================================

def read_datasus_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo DATASUS não encontrado: {path}")

    for sep, enc in [(";", "latin1"), (",", "latin1"), (";", "utf-8"), (",", "utf-8")]:
        try:
            df = pd.read_csv(path, sep=sep, encoding=enc)
            if len(df.columns) >= 1:
                return df
        except Exception:
            continue
    return pd.read_csv(path)


def read_csv_default(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    return pd.read_csv(path)


# =====================================
# Construção da estrutura OMS (comunitária)
# =====================================

def build_structured(chapters: pd.DataFrame, blocks: pd.DataFrame,
                     categories: pd.DataFrame, subcats: pd.DataFrame) -> pd.DataFrame:
    # Primeiro conecta categorias aos capítulos para evitar conflito de nomes
    cats = categories.merge(chapters, on="chapter_code", how="left")
    # Em seguida conecta aos blocos
    cats = cats.merge(blocks, on="block_id", how="left")
    # Em alguns casos, o merge com blocos pode criar colunas com sufixos para 'chapter_code'
    if "chapter_code" not in cats.columns and ("chapter_code_x" in cats.columns or "chapter_code_y" in cats.columns):
        cats["chapter_code"] = cats.get("chapter_code_x")
        if "chapter_code_y" in cats.columns:
            # se existir ambos, usa o de categorias (x) como preferencial, com fallback para o de blocos (y)
            cats["chapter_code"] = cats["chapter_code"].fillna(cats["chapter_code_y"])
        # limpar colunas com sufixo se presentes
        for col in ["chapter_code_x", "chapter_code_y"]:
            if col in cats.columns:
                cats.drop(columns=[col], inplace=True)

    # Expandir subcategorias conectando na categoria
    full = subcats.merge(
        cats,
        left_on="category_code",
        right_on="category_code",
        how="left"
    )

    # Normalizações e renomeações
    full["cid_codigo"] = full["subcategory_code"].apply(normalize_code)
    full["cid_categoria"] = full["category_code"].apply(normalize_code)
    full["cid_subcategoria"] = full["cid_codigo"].apply(lambda c: c if c and "." in c else None)

    # Título curto e descrição
    # Quando disponível, usamos subcategory_title como título/descrição
    title_col = "subcategory_title" if "subcategory_title" in full.columns else None
    full["titulo"] = full[title_col] if title_col else full.get("category_title", pd.Series(index=full.index))
    full["descricao"] = full["titulo"]

    # Campos de hierarquia
    full["bloco_codigo"] = full.get("block_id")
    full["bloco_titulo"] = full.get("block_title")
    full["capitulo_codigo"] = full.get("chapter_code")
    full["capitulo_titulo"] = full.get("chapter_title")

    full["fonte"] = "Estruturada"

    # Seleção e ordenação de colunas finais
    cols = [
        "cid_codigo", "cid_categoria", "cid_subcategoria", "titulo", "descricao",
        "capitulo_codigo", "capitulo_titulo", "bloco_codigo", "bloco_titulo", "fonte"
    ]
    return full[cols]


# =====================================
# Enriquecimento DATASUS
# =====================================

def prepare_datasus(raw: pd.DataFrame, cats: pd.DataFrame) -> pd.DataFrame:
    # Normalizar nomes conforme esperado
    col_map = {}
    if "codigo" in raw.columns:
        col_map["codigo"] = "cid_codigo"
    if "descricao" in raw.columns:
        col_map["descricao"] = "descricao"
    raw = raw.rename(columns=col_map)

    # Se já vier com cid_codigo/descricao, garantimos presença
    if "cid_codigo" not in raw.columns:
        # tenta detectar a primeira coluna como código
        first_col = raw.columns[0]
        raw = raw.rename(columns={first_col: "cid_codigo"})
    if "descricao" not in raw.columns:
        # cria coluna vazia caso inexistente
        raw["descricao"] = None

    # Normalizações
    raw["cid_codigo"] = raw["cid_codigo"].apply(normalize_code)
    raw["cid_categoria"] = raw["cid_codigo"].apply(extract_root_category)
    raw["cid_subcategoria"] = raw["cid_codigo"].apply(lambda c: c if c and "." in c else None)
    raw["titulo"] = raw["descricao"]

    # cats deve conter pelo menos: category_code, block_id, block_title, chapter_code, chapter_title
    cats_norm = cats.copy()
    cats_norm["category_code"] = cats_norm["category_code"].apply(normalize_code)

    enriched = raw.merge(
        cats_norm[["category_code", "block_id", "block_title", "chapter_code", "chapter_title"]],
        left_on="cid_categoria",
        right_on="category_code",
        how="left"
    )

    enriched["bloco_codigo"] = enriched.get("block_id")
    enriched["bloco_titulo"] = enriched.get("block_title")
    enriched["capitulo_codigo"] = enriched.get("chapter_code")
    enriched["capitulo_titulo"] = enriched.get("chapter_title")

    enriched["fonte"] = "DATASUS"

    # Seleção final
    cols = [
        "cid_codigo", "cid_categoria", "cid_subcategoria", "titulo", "descricao",
        "capitulo_codigo", "capitulo_titulo", "bloco_codigo", "bloco_titulo", "fonte"
    ]
    return enriched[cols]


# =====================================
# ETL principal
# =====================================

def run_etl(datasus_path: str, chapters_path: str, blocks_path: str,
            categories_path: str, subcats_path: str, out_path: str) -> str:
    # Carregar fontes
    datasus_raw = read_datasus_csv(datasus_path)
    chapters = read_csv_default(chapters_path)
    blocks = read_csv_default(blocks_path)
    categories = read_csv_default(categories_path)
    subcats = read_csv_default(subcats_path)

    # Construir estrutura OMS
    structured_full = build_structured(chapters, blocks, categories, subcats)

    # Para enriquecer DATASUS, precisamos do mapa de categorias
    cats = categories.merge(blocks, on="block_id", how="left").merge(chapters, on="chapter_code", how="left")
    datasus_enriched = prepare_datasus(datasus_raw, cats)

    # Unificar e deduplicar priorizando estruturada
    df = pd.concat([structured_full, datasus_enriched], ignore_index=True)
    df["cid_codigo"] = df["cid_codigo"].apply(normalize_code)

    # Ordena por fonte para manter "Estruturada" antes de "DATASUS"
    df = df.sort_values("fonte", ascending=False)
    df = df.drop_duplicates(subset=["cid_codigo"], keep="first")

    # Data de atualização
    df["dt_atualizacao"] = datetime.now().strftime("%Y-%m-%d")

    # Qualidade básica
    total = len(df)
    missing_hier = df[["bloco_codigo", "capitulo_codigo"]].isna().any(axis=1).sum()
    print(f"Total de códigos consolidados: {total}")
    print(f"Registros sem bloco/capítulo após merge: {missing_hier}")

    # Exportar
    df.to_csv(out_path, index=False, sep=';', encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    return out_path


# =====================================
# Leitura dos CSV oficiais do DataSUS (CAPITULOS/GRUPOS/CATEGORIAS/SUBCATEGORIAS)
# =====================================

def _find_file_by_fragment(dir_path: str, fragment: str) -> str:
    files = os.listdir(dir_path)
    for f in files:
        if fragment.lower() in f.lower() and f.lower().endswith('.csv'):
            return os.path.join(dir_path, f)
    raise FileNotFoundError(f"Não encontrado CSV com fragmento '{fragment}' em {dir_path}")


def read_datasus_official(dir_path: str):
    if not os.path.isdir(dir_path):
        raise FileNotFoundError(f"Diretório não encontrado: {dir_path}")

    enc = 'latin1'
    sep = ';'
    # Detecta arquivos pelos nomes usuais
    chapters_path = _find_file_by_fragment(dir_path, 'CAPITULOS')
    blocks_path = _find_file_by_fragment(dir_path, 'GRUPOS')
    categories_path = _find_file_by_fragment(dir_path, 'CATEGORIAS')
    subcats_path = _find_file_by_fragment(dir_path, 'SUBCATEGORIAS')

    chapters_raw = pd.read_csv(chapters_path, sep=sep, encoding=enc)
    blocks_raw = pd.read_csv(blocks_path, sep=sep, encoding=enc)
    categories_raw = pd.read_csv(categories_path, sep=sep, encoding=enc)
    subcats_raw = pd.read_csv(subcats_path, sep=sep, encoding=enc)

    # Mapear para esquema estruturado esperado
    chapters = pd.DataFrame({
        'chapter_code': chapters_raw['CATINIC'].astype(str).str.strip() + '-' + chapters_raw['CATFIM'].astype(str).str.strip(),
        'chapter_title': chapters_raw['DESCRICAO'].astype(str).str.strip(),
    })

    blocks = pd.DataFrame({
        'block_id': blocks_raw['CATINIC'].astype(str).str.strip() + '-' + blocks_raw['CATFIM'].astype(str).str.strip(),
        'block_title': blocks_raw['DESCRICAO'].astype(str).str.strip(),
        'chapter_code': None,  # será inferido juntando pela faixa mais tarde, se necessário
    })

    categories = pd.DataFrame({
        'category_code': categories_raw['CAT'].astype(str).str.strip().str.upper(),
        'category_title': categories_raw['DESCRICAO'].astype(str).str.strip(),
        'block_id': None,  # inferência por faixa
        'chapter_code': None,  # inferência por faixa
    })

    # Construir código da subcategoria com ponto quando houver 4º dígito
    def _format_subcat(sc: str) -> str:
        if pd.isna(sc):
            return None
        s = str(sc).strip().upper()
        if len(s) >= 4 and s[3] != '':
            # alguns CSVs vêm com espaço em branco na 4ª posição quando não há subcategoria
            fourth = s[3].strip()
            if fourth:
                return s[:3] + '.' + s[3:]
        return s[:3]

    subcats = pd.DataFrame({
        'subcategory_code': subcats_raw['SUBCAT'].apply(_format_subcat),
        'subcategory_title': subcats_raw['DESCRICAO'].astype(str).str.strip() if 'DESCRICAO' in subcats_raw.columns else subcats_raw.get('DESCRABREV', pd.Series(index=subcats_raw.index)).astype(str).str.strip(),
        'category_code': subcats_raw['SUBCAT'].astype(str).str[:3].str.upper(),
    })

    # Inferir block_id e chapter_code para categories a partir de faixas CATINIC-CATFIM
    # Cria intervalos para busca
    def _belongs_to_range(code3: str, start: str, end: str) -> bool:
        return start <= code3 <= end

    # Mapear categoria para bloco
    block_ranges = blocks_raw[['CATINIC', 'CATFIM']].astype(str).apply(lambda s: s.str.strip().str.upper())
    block_ranges = list(zip(block_ranges['CATINIC'], block_ranges['CATFIM']))
    block_titles = blocks_raw['DESCRICAO'].astype(str).str.strip().tolist()

    # Mapear categoria para capítulo
    chapter_ranges = chapters_raw[['CATINIC', 'CATFIM']].astype(str).apply(lambda s: s.str.strip().str.upper())
    chapter_ranges = list(zip(chapter_ranges['CATINIC'], chapter_ranges['CATFIM']))
    chapter_titles = chapters_raw['DESCRICAO'].astype(str).str.strip().tolist()

    # Construir dicionários para rápida inferência
    block_map = {}
    for (start, end), title in zip(block_ranges, block_titles):
        block_map[(start, end)] = {
            'block_id': f"{start}-{end}",
            'block_title': title,
        }

    chapter_map = {}
    for (start, end), title in zip(chapter_ranges, chapter_titles):
        chapter_map[(start, end)] = {
            'chapter_code': f"{start}-{end}",
            'chapter_title': title,
        }

    # Preencher block_id e chapter_code nas categorias
    cat_codes = categories['category_code'].tolist()
    inferred_block_ids = []
    inferred_chapter_codes = []
    for code in cat_codes:
        b_id = None
        c_code = None
        for (start, end), binfo in block_map.items():
            if _belongs_to_range(code, start, end):
                b_id = binfo['block_id']
                break
        for (start, end), cinfo in chapter_map.items():
            if _belongs_to_range(code, start, end):
                c_code = cinfo['chapter_code']
                break
        inferred_block_ids.append(b_id)
        inferred_chapter_codes.append(c_code)
    categories['block_id'] = inferred_block_ids
    categories['chapter_code'] = inferred_chapter_codes

    # Também vincular blocks aos capítulos pelas faixas
    inferred_block_chapters = []
    for (start, end) in block_ranges:
        c_code = None
        for (cstart, cend), cinfo in chapter_map.items():
            if _belongs_to_range(start, cstart, cend) or _belongs_to_range(end, cstart, cend):
                c_code = cinfo['chapter_code']
                break
        inferred_block_chapters.append(c_code)
    blocks['chapter_code'] = inferred_block_chapters

    return chapters, blocks, categories, subcats

# =====================================
# ETL usando apenas CSV oficiais do DataSUS
# =====================================

def run_etl_from_datasus_dir(datasus_dir: str, out_path: str) -> str:
    chapters, blocks, categories, subcats = read_datasus_official(datasus_dir)

    # Estrutura completa com hierarquia
    structured_full = build_structured(chapters, blocks, categories, subcats)

    # Preparar base DATASUS a partir das subcategorias oficiais
    cats = categories.merge(chapters, on="chapter_code", how="left").merge(blocks, on="block_id", how="left")
    # Normalizar possíveis sufixos de chapter_code criados pelo merge
    if "chapter_code" not in cats.columns and ("chapter_code_x" in cats.columns or "chapter_code_y" in cats.columns):
        cats["chapter_code"] = cats.get("chapter_code_x")
        if "chapter_code_y" in cats.columns:
            cats["chapter_code"] = cats["chapter_code"].fillna(cats["chapter_code_y"])
        for col in ["chapter_code_x", "chapter_code_y"]:
            if col in cats.columns:
                cats.drop(columns=[col], inplace=True)

    datasus_raw = pd.DataFrame({
        'codigo': structured_full['cid_codigo'],
        'descricao': structured_full['descricao'],
    })
    datasus_enriched = prepare_datasus(datasus_raw, cats)

    # Unificar e deduplicar priorizando estruturada
    df = pd.concat([structured_full, datasus_enriched], ignore_index=True)
    df["cid_codigo"] = df["cid_codigo"].apply(normalize_code)

    df = df.sort_values("fonte", ascending=False)
    df = df.drop_duplicates(subset=["cid_codigo"], keep="first")

    df["dt_atualizacao"] = datetime.now().strftime("%Y-%m-%d")

    total = len(df)
    missing_hier = df[["bloco_codigo", "capitulo_codigo"]].isna().any(axis=1).sum()
    print(f"Total de códigos consolidados: {total}")
    print(f"Registros sem bloco/capítulo após merge: {missing_hier}")

    df.to_csv(out_path, index=False, sep=';', encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="ETL CID-10 (Fluap): combina DATASUS e fonte estruturada OMS")
    parser.add_argument("--datasus", required=False, help="Caminho para CSV oficial DATASUS (colunas: codigo; descricao)")
    parser.add_argument("--chapters", required=False, help="Caminho para chapters.csv (OMS estruturada)")
    parser.add_argument("--blocks", required=False, help="Caminho para blocks.csv (OMS estruturada)")
    parser.add_argument("--categories", required=False, help="Caminho para categories.csv (OMS estruturada)")
    parser.add_argument("--subcategories", required=False, help="Caminho para subcategories.csv (OMS estruturada)")
    parser.add_argument("--datasus_dir", required=False, help="Diretório com os CSV oficiais do DataSUS (CAPITULOS/GRUPOS/CATEGORIAS/SUBCATEGORIAS)")
    parser.add_argument("--out", default="cid10_consolidado.csv", help="Arquivo de saída (CSV)")

    args = parser.parse_args()

    if args.datasus_dir:
        out_file = run_etl_from_datasus_dir(
            datasus_dir=args.datasus_dir,
            out_path=args.out,
        )
    else:
        # Validação mínima para o modo antigo
        required = [args.datasus, args.chapters, args.blocks, args.categories, args.subcategories]
        if not all(required):
            raise SystemExit("Parâmetros insuficientes. Informe --datasus_dir OU todas as paths de --datasus/--chapters/--blocks/--categories/--subcategories")
        out_file = run_etl(
            datasus_path=args.datasus,
            chapters_path=args.chapters,
            blocks_path=args.blocks,
            categories_path=args.categories,
            subcats_path=args.subcategories,
            out_path=args.out,
        )
    print(f"Arquivo exportado: {out_file}")


if __name__ == "__main__":
    main()