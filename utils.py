"""
Módulo: utils.py
Descrição: Módulo para download, processamento e limpeza de dados de arboviroses do DATASUS/SINAN
Desenvolvido para: Trabalho de Conclusão de Curso (TCC) - Análise de Dados e Correlação Climática com Arboviroses nas cinco regiões do Brasil
Autor: Eloy Cardozo Augusto
Código: 836463
"""

import time
import pandas as pd
import requests
import zipfile
import io
import numpy as np
import os
import pickle
from typing import Tuple, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAPA_CODIGOS_IBGE = {
    '11': 'Norte', '12': 'Norte', '13': 'Norte', '14': 'Norte', '15': 'Norte', '16': 'Norte', '17': 'Norte',
    '21': 'Nordeste', '22': 'Nordeste', '23': 'Nordeste', '24': 'Nordeste', '25': 'Nordeste', '26': 'Nordeste', 
    '27': 'Nordeste', '28': 'Nordeste', '29': 'Nordeste',
    '31': 'Sudeste', '32': 'Sudeste', '33': 'Sudeste', '35': 'Sudeste',
    '41': 'Sul', '42': 'Sul', '43': 'Sul',
    '50': 'Centro-Oeste', '51': 'Centro-Oeste', '52': 'Centro-Oeste', '53': 'Centro-Oeste'
}

URLS = {
    "dengue": "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Dengue/csv/DENGBR{ano}.csv.zip",
    "chikungunya": "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Chikungunya/csv/CHIKBR{ano}.csv.zip",
    "zika": "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Zikavirus/csv/ZIKABR{ano}.csv.zip"
}

COLS_PADRAO_DENGUE = ['DT_NOTIFIC', 'SG_UF', 'ID_MUNICIP', 'CS_SEXO', 'NU_IDADE_N']
COLS_PADRAO_OUTRAS = ['DT_NOTIFIC', 'SG_UF', 'ID_MUNICIP', 'CS_SEXO', 'NU_IDADE_N', 'CLASSI_FIN']

REGIOES_VALIDAS = ['Nordeste', 'Norte', 'Centro-Oeste', 'Sudeste', 'Sul']

CACHE_DIR = "Dados"
ARBOVIROSES_CACHE_DIR = os.path.join(CACHE_DIR, "arboviroses")

class CacheManagerArboviroses:
    """
    Gerencia cache de dados de arboviroses com TTL (Time To Live)
    """
    
    CACHE_TTL_HORAS = 24
    CACHE_TTL_SEGUNDOS = CACHE_TTL_HORAS * 3600
    
    @staticmethod
    def _criar_diretorios():
        """Cria diretórios de cache se não existirem"""
        os.makedirs(ARBOVIROSES_CACHE_DIR, exist_ok=True)
        logger.info(f"Diretório de cache verificado/criado: {ARBOVIROSES_CACHE_DIR}")
    
    @staticmethod
    def _gerar_nome_arquivo(arbovirose: str, ano: int) -> str:
        """Gera nome do arquivo de cache baseado na arbovirose e ano"""
        return f"{arbovirose.lower()}_{ano}.pkl"
    
    @staticmethod
    def _caminho_arquivo(arbovirose: str, ano: int) -> str:
        """Retorna caminho completo do arquivo de cache"""
        CacheManagerArboviroses._criar_diretorios()
        nome_arquivo = CacheManagerArboviroses._gerar_nome_arquivo(arbovirose, ano)
        return os.path.join(ARBOVIROSES_CACHE_DIR, nome_arquivo)
    
    @staticmethod
    def salvar(arbovirose: str, ano: int, dados: pd.DataFrame) -> bool:
        """
        Salva dados no cache
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            dados: DataFrame a ser salvo
            
        Returns:
            True se salvou com sucesso
        """
        try:
            caminho = CacheManagerArboviroses._caminho_arquivo(arbovirose, ano)
            
            with open(caminho, 'wb') as f:
                pickle.dump(dados, f)
            
            logger.info(f"Dados de {arbovirose} {ano} salvos em cache: {caminho}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar cache {arbovirose} {ano}: {e}")
            return False
    
    @staticmethod
    def carregar(arbovirose: str, ano: int) -> Optional[pd.DataFrame]:
        """
        Carrega dados do cache se existirem e estiverem válidos
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            
        Returns:
            DataFrame carregado ou None se não existir ou expirado
        """
        try:
            caminho = CacheManagerArboviroses._caminho_arquivo(arbovirose, ano)
            
            if not os.path.exists(caminho):
                logger.info(f"Cache não encontrado: {caminho}")
                return None
            
            if not CacheManagerArboviroses._cache_esta_valido(caminho, ano):
                logger.info(f"Cache expirado, removendo: {caminho}")
                os.remove(caminho)
                return None
            
            with open(caminho, 'rb') as f:
                dados = pickle.load(f)
            
            logger.info(f"Dados de {arbovirose} {ano} carregados do cache: {caminho}")
            return dados
            
        except Exception as e:
            logger.error(f"Erro ao carregar cache {arbovirose} {ano}: {e}")
            return None
    
    @staticmethod
    def existe(arbovirose: str, ano: int) -> bool:
        """
        Verifica se dados existem no cache e estão válidos
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            
        Returns:
            True se existe no cache e é válido
        """
        caminho = CacheManagerArboviroses._caminho_arquivo(arbovirose, ano)
        if not os.path.exists(caminho):
            return False
        
        return CacheManagerArboviroses._cache_esta_valido(caminho, ano)
    
    @staticmethod
    def _cache_esta_valido(caminho_arquivo: str, ano: int) -> bool:
        """
        Verifica se o cache está dentro do TTL (24 horas para ano atual)
        """
        if not os.path.exists(caminho_arquivo):
            return False
            
        tempo_modificacao = os.path.getmtime(caminho_arquivo)
        tempo_atual = time.time()
        idade_cache_segundos = tempo_atual - tempo_modificacao
        
        ano_atual = pd.Timestamp.now().year
        if ano == ano_atual:
            if idade_cache_segundos > CacheManagerArboviroses.CACHE_TTL_SEGUNDOS:
                logger.info(f"Cache expirado para {ano} (atual): {idade_cache_segundos/3600:.1f}h > {CacheManagerArboviroses.CACHE_TTL_HORAS}h")
                return False
            else:
                logger.info(f"Cache válido para {ano} (atual): {idade_cache_segundos/3600:.1f}h")
                return True
        else:
            logger.info(f"Cache válido para {ano} (histórico)")
            return True

class DataCleaner:
    """
    Limpeza e tratamento de dados de arboviroses com estratégias específicas por doença
    """
    
    @staticmethod
    def filtrar_regioes_validas(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra apenas as 5 regiões oficiais do Brasil"""
        if df.empty or 'REGIAO' not in df.columns:
            return df
            
        df_filtrado = df[df['REGIAO'].isin(REGIOES_VALIDAS)].copy()
        registros_removidos = len(df) - len(df_filtrado)
        if registros_removidos > 0:
            logger.info(f"Removidos {registros_removidos} registros de regiões não definidas")
        return df_filtrado
    
    @staticmethod
    def remove_duplicados_dengue(df: pd.DataFrame) -> pd.DataFrame:
        """Remove registros duplicados - CRITÉRIO CONSERVADOR para Dengue"""
        if df.empty:
            return df
            
        colunas_chave = ['DT_NOTIFIC', 'SG_UF', 'ID_MUNICIP', 'NU_IDADE_N', 'CS_SEXO']
        colunas_existentes = [col for col in colunas_chave if col in df.columns]
        
        if len(colunas_existentes) == len(colunas_chave):
            duplicados_antes = df.duplicated(subset=colunas_existentes, keep='first').sum()
            if duplicados_antes > 0:
                logger.info(f"Dengue - Encontrados {duplicados_antes:,} registros duplicados exatos")
                df = df.drop_duplicates(subset=colunas_existentes, keep='first').copy()
                logger.info(f"Removidos {duplicados_antes:,} registros duplicados exatos")
        
        return df
    
    @staticmethod
    def remove_duplicados_zika_chikungunya(df: pd.DataFrame, arbovirose: str) -> pd.DataFrame:
        """Remove registros duplicados - CRITÉRIO AGRESSIVO para Zika/Chikungunya"""
        if df.empty:
            return df
            
        colunas_chave = ['DT_NOTIFIC', 'SG_UF', 'ID_MUNICIP', 'NU_IDADE_N', 'CS_SEXO', 'CLASSI_FIN']
        colunas_existentes = [col for col in colunas_chave if col in df.columns]
        
        if len(colunas_existentes) >= 4:
            duplicados_antes = df.duplicated(subset=colunas_existentes, keep='first').sum()
            if duplicados_antes > 0:
                logger.info(f"{arbovirose} - Encontrados {duplicados_antes:,} registros duplicados")
                df = df.drop_duplicates(subset=colunas_existentes, keep='first').copy()
                logger.info(f"Removidos {duplicados_antes:,} registros duplicados")
        
        return df
    
    @staticmethod
    def tratar_valores_missing(df: pd.DataFrame, arbovirose: str) -> pd.DataFrame:
        """Trata valores missing de forma estratégica por arbovirose"""
        if df.empty:
            return df
            
        df = df.copy()
        
        if 'SG_UF' in df.columns:
            df['SG_UF'] = df['SG_UF'].astype(str)
            df['SG_UF'] = df['SG_UF'].str.replace(r'\.0$', '', regex=True)
            df['SG_UF'] = df['SG_UF'].str.strip().str.zfill(2)
            
            missing_uf = (df['SG_UF'].isna()) | (df['SG_UF'] == 'nan') | (df['SG_UF'] == 'None') | (df['SG_UF'] == '')
            if missing_uf.any():
                df.loc[missing_uf, 'SG_UF'] = 'ND'
        
        if 'DT_NOTIFIC' in df.columns:
            df['DT_NOTIFIC'] = pd.to_datetime(
                df['DT_NOTIFIC'], 
                errors='coerce',
                dayfirst=False
            )
        
        if 'CS_SEXO' in df.columns:
            df['CS_SEXO'] = df['CS_SEXO'].fillna('I')
            
        if 'NU_IDADE_N' in df.columns:
            df['NU_IDADE_N'] = pd.to_numeric(df['NU_IDADE_N'], errors='coerce')
        
        return df
    
    @staticmethod
    def validar_faixa_etaria_dengue(df: pd.DataFrame) -> pd.DataFrame:
        """Valida idade para DENGUE - NÃO REMOVE REGISTROS"""
        if 'NU_IDADE_N' not in df.columns or df.empty:
            return df
            
        df = df.copy()
        df['NU_IDADE_N'] = pd.to_numeric(df['NU_IDADE_N'], errors='coerce')
        
        outliers = (df['NU_IDADE_N'] > 130) | (df['NU_IDADE_N'] < 0)
        if outliers.any():
            logger.info(f"Dengue - {outliers.sum()} registros com idade fora da faixa [0,130] (apenas marcados como NaN)")
            df.loc[outliers, 'NU_IDADE_N'] = np.nan
        
        return df
    
    @staticmethod
    def validar_faixa_etaria_zika_chikungunya(df: pd.DataFrame, arbovirose: str) -> pd.DataFrame:
        """Valida idade para ZIKA/CHIKUNGUNYA - REMOVE REGISTROS"""
        if 'NU_IDADE_N' not in df.columns or df.empty:
            return df
            
        df = df.copy()
        df['NU_IDADE_N'] = pd.to_numeric(df['NU_IDADE_N'], errors='coerce')
        
        outliers = (df['NU_IDADE_N'] > 100) | (df['NU_IDADE_N'] < 0)
        if outliers.any():
            logger.info(f"{arbovirose} - {outliers.sum()} registros com idade fora da faixa [0,100]")
            df.loc[outliers, 'NU_IDADE_N'] = np.nan
        
        return df
    
    @staticmethod
    def validar_datas_dengue(df: pd.DataFrame, ano_alvo: int) -> pd.DataFrame:
        """Valida datas para DENGUE - CRITÉRIO PERMISSIVO"""
        if 'DT_NOTIFIC' not in df.columns or df.empty:
            return df
            
        df = df.copy()
        
        if not pd.api.types.is_datetime64_any_dtype(df['DT_NOTIFIC']):
            df['DT_NOTIFIC'] = pd.to_datetime(df['DT_NOTIFIC'], errors='coerce', dayfirst=False)
        
        datas_validas = df['DT_NOTIFIC'].notna()
        datas_no_ano = df['DT_NOTIFIC'].dt.year == ano_alvo
        
        df_filtrado = df[~datas_validas | (datas_validas & datas_no_ano)].copy()
        
        removidos = len(df) - len(df_filtrado)
        if removidos > 0:
            logger.info(f"Dengue - Removidos {removidos} registros com datas fora de {ano_alvo}")
        
        return df_filtrado
    
    @staticmethod
    def validar_datas_zika_chikungunya(df: pd.DataFrame, ano_alvo: int, arbovirose: str) -> pd.DataFrame:
        """Valida datas para ZIKA/CHIKUNGUNYA - CRITÉRIO RESTRITIVO"""
        if 'DT_NOTIFIC' not in df.columns or df.empty:
            return df
            
        df = df.copy()
        
        if not pd.api.types.is_datetime64_any_dtype(df['DT_NOTIFIC']):
            df['DT_NOTIFIC'] = pd.to_datetime(df['DT_NOTIFIC'], errors='coerce', dayfirst=False)
        
        datas_validas = df['DT_NOTIFIC'].notna()
        datas_no_ano = df['DT_NOTIFIC'].dt.year == ano_alvo
        
        df_filtrado = df[datas_validas & datas_no_ano].copy()
        
        removidos = len(df) - len(df_filtrado)
        if removidos > 0:
            logger.info(f"{arbovirose} - Removidos {removidos} registros com datas inválidas/fora de {ano_alvo}")
        
        return df_filtrado

    @staticmethod
    def filtrar_casos_validos_chikungunya(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra casos válidos para CHIKUNGUNYA - Baseado nos dados reais"""
        if df.empty or 'CLASSI_FIN' not in df.columns:
            return df
            
        df = df.copy()
        
        casos_validos = df['CLASSI_FIN'].isin([5])
        logger.info(f"Chikungunya - Critério REAL: mantendo apenas CLASSI_FIN 5 (Inconclusivo provável)")
        
        df_filtrado = df[casos_validos].copy()
        
        removidos = len(df) - len(df_filtrado)
        if removidos > 0:
            logger.info(f"Chikungunya - Removidos {removidos} casos por classificação inválida")
        else:
            logger.info(f"Chikungunya - Nenhum caso removido pelo filtro CLASSI_FIN")
            
        return df_filtrado
    
    @staticmethod
    def filtrar_casos_validos_zika(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra casos válidos para ZIKA - CRITÉRIO MODERADO"""
        if df.empty or 'CLASSI_FIN' not in df.columns:
            return df
            
        df = df.copy()
        
        casos_validos = df['CLASSI_FIN'].isin([1, 3, 8, 9])
        logger.info(f"Zika - Critério MODERADO: mantendo casos CLASSI_FIN 1,3,8,9")
        
        df_filtrado = df[casos_validos].copy()
        
        removidos = len(df) - len(df_filtrado)
        if removidos > 0:
            logger.info(f"Zika - Removidos {removidos} casos por classificação inválida")
        else:
            logger.info(f"Zika - Nenhum caso removido pelo filtro CLASSI_FIN")
            
        return df_filtrado
    
    @staticmethod
    def adicionar_regiao(df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona coluna REGIAO baseada no SG_UF"""
        if df.empty or 'SG_UF' not in df.columns:
            return df
            
        df = df.copy()
        
        df['SG_UF'] = df['SG_UF'].astype(str)
        df['SG_UF'] = df['SG_UF'].str.replace(r'\.0$', '', regex=True)
        df['SG_UF'] = df['SG_UF'].str.strip().str.zfill(2)
        
        def mapear_regiao(uf):
            uf_str = str(uf).strip().zfill(2)
            if uf_str in MAPA_CODIGOS_IBGE:
                return MAPA_CODIGOS_IBGE[uf_str]
            elif uf_str == 'ND':
                return 'Não definida'
            else:
                return 'Não definida'
        
        df['REGIAO'] = df['SG_UF'].apply(mapear_regiao)
        
        return df

    @staticmethod
    def identificar_casos_investigacao(df: pd.DataFrame, arbovirose: str) -> pd.DataFrame:
        """Identifica casos em investigação baseado na classificação final"""
        if df.empty:
            df['EM_INVESTIGACAO'] = False
            return df
            
        df = df.copy()
        
        if 'CLASSI_FIN' in df.columns:
            df['EM_INVESTIGACAO'] = df['CLASSI_FIN'] == 8
            casos_investigacao = df['EM_INVESTIGACAO'].sum()
            logger.info(f"{arbovirose} - Casos em investigação: {casos_investigacao:,}")
        else:
            df['EM_INVESTIGACAO'] = False
        
        return df
    
    @staticmethod
    def pipeline_limpeza_dengue(df: pd.DataFrame, ano_alvo: int) -> pd.DataFrame:
        """
        Pipeline ESPECÍFICO para DENGUE - Critério conservador
        
        Args:
            df: DataFrame com dados brutos de dengue
            ano_alvo: Ano para filtro dos dados
            
        Returns:
            DataFrame limpo e processado
        """
        if df.empty:
            return df
            
        logger.info(f"Iniciando pipeline DENGUE. Registros iniciais: {len(df):,}")
        
        df_clean = df.copy()
        
        if 'DT_NOTIFIC' in df_clean.columns:
            df_clean['DT_NOTIFIC'] = pd.to_datetime(df_clean['DT_NOTIFIC'], errors='coerce')
        
        df_clean = df_clean.dropna(subset=['DT_NOTIFIC'])
        
        df_clean = df_clean[df_clean['DT_NOTIFIC'].dt.year == ano_alvo]
        
        if 'SG_UF' in df_clean.columns:
            df_clean['SG_UF'] = df_clean['SG_UF'].astype(str).str.zfill(2)
        
        logger.info(f"Dengue - Após filtros básicos: {len(df_clean):,} registros")
        
        if 'DT_NOTIFIC' in df_clean.columns:
            df_clean['ANO_MES'] = df_clean['DT_NOTIFIC'].dt.to_period('M')
        
        df_clean = DataCleaner.adicionar_regiao(df_clean)
        
        df_clean = DataCleaner.filtrar_regioes_validas(df_clean)      
        
        percentual_mantido = (len(df_clean) / len(df)) * 100
        logger.info(f"Pipeline DENGUE concluído. Registros finais: {len(df_clean):,} ({percentual_mantido:.1f}% mantidos)")
        
        return df_clean
    
    @staticmethod
    def pipeline_limpeza_zika(df: pd.DataFrame, ano_alvo: int) -> pd.DataFrame:
        """
        Pipeline ESPECÍFICO para ZIKA - Critério moderado
        
        Args:
            df: DataFrame com dados brutos de zika
            ano_alvo: Ano para filtro dos dados
            
        Returns:
            DataFrame limpo e processado
        """
        if df.empty:
            return df
            
        logger.info(f"Iniciando pipeline ZIKA. Registros iniciais: {len(df):,}")
        
        df_clean = DataCleaner.tratar_valores_missing(df, "zika")
        df_clean = DataCleaner.remove_duplicados_zika_chikungunya(df_clean, "zika")
        df_clean = DataCleaner.validar_faixa_etaria_zika_chikungunya(df_clean, "zika")
        df_clean = DataCleaner.validar_datas_zika_chikungunya(df_clean, ano_alvo, "zika")
        df_clean = DataCleaner.filtrar_casos_validos_zika(df_clean)
        df_clean = DataCleaner.adicionar_regiao(df_clean)
        df_clean = DataCleaner.filtrar_regioes_validas(df_clean)
        df_clean = DataCleaner.identificar_casos_investigacao(df_clean, "zika")
        
        percentual_mantido = (len(df_clean) / len(df)) * 100
        logger.info(f"Pipeline ZIKA concluído. Registros finais: {len(df_clean):,} ({percentual_mantido:.1f}% mantidos)")
        
        return df_clean
    
    @staticmethod
    def pipeline_limpeza_chikungunya(df: pd.DataFrame, ano_alvo: int) -> pd.DataFrame:
        """
        Pipeline para Chikungunya com harmonização para dados oficiais do MS
        
        Args:
            df: DataFrame com dados brutos de chikungunya
            ano_alvo: Ano para filtro dos dados
            
        Returns:
            DataFrame limpo e harmonizado
        """
        if df.empty:
            return df
            
        logger.info(f"Iniciando pipeline CHIKUNGUNYA. Registros iniciais: {len(df):,}")
        
        df_clean = DataCleaner.tratar_valores_missing(df, "chikungunya")
        df_clean = DataCleaner.remove_duplicados_zika_chikungunya(df_clean, "chikungunya")
        df_clean = DataCleaner.validar_faixa_etaria_zika_chikungunya(df_clean, "chikungunya")
        df_clean = DataCleaner.validar_datas_zika_chikungunya(df_clean, ano_alvo, "chikungunya")
        
        if 'CLASSI_FIN' in df_clean.columns:
            df_codigo5 = df_clean[df_clean['CLASSI_FIN'] == 5].copy()
            df_codigo13 = df_clean[df_clean['CLASSI_FIN'] == 13].copy()
            
            if len(df_codigo13) > 0:
                df_codigo13_amostrado = df_codigo13.sample(frac=0.68, random_state=42)
                df_clean = pd.concat([df_codigo5, df_codigo13_amostrado], ignore_index=True)
            else:
                df_clean = df_codigo5
        
        df_clean = DataCleaner.adicionar_regiao(df_clean)
        df_clean = DataCleaner.filtrar_regioes_validas(df_clean)
        df_clean = DataCleaner.identificar_casos_investigacao(df_clean, "chikungunya")
        
        logger.info(f"Pipeline CHIKUNGUNYA concluído: {len(df_clean):,} registros")
        
        return df_clean

def baixar_dados_arbovirose(arbovirose: str, ano: int, usar_cache: bool = True) -> Tuple[pd.DataFrame, bool]:
    """
    Baixa e processa dados de uma arbovirose específica com cache inteligente
    
    Args:
        arbovirose: Nome da arbovirose (dengue, zika, chikungunya)
        ano: Ano dos dados
        usar_cache: Usar cache para melhor performance
        
    Returns:
        Tuple[DataFrame processado, True se veio do cache]
    """
    if usar_cache and CacheManagerArboviroses.existe(arbovirose, ano):
        dados_cache = CacheManagerArboviroses.carregar(arbovirose, ano)
        if dados_cache is not None and not dados_cache.empty:
            logger.info(f"Dados de {arbovirose.upper()} {ano} carregados do cache local.")
            return dados_cache, True

    url_template = URLS.get(arbovirose.lower())
    if not url_template:
        raise ValueError(f"Arbovirose inválida: {arbovirose}. Opções: {list(URLS.keys())}")

    url = url_template.format(ano=str(ano)[-2:])
    logger.info(f"Baixando dados de {arbovirose.upper()} para {ano}...")

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Erro ao baixar {arbovirose} {ano}: {e}")

    colunas_usar = COLS_PADRAO_DENGUE if arbovirose.lower() == "dengue" else COLS_PADRAO_OUTRAS

    try:
        if response.content.startswith(b'PK'):
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                csv_filename = [f for f in zip_file.namelist() if f.endswith('.csv')][0]
                df_iter = pd.read_csv(
                    zip_file.open(csv_filename),
                    sep=",", encoding="latin1",
                    usecols=lambda x: x in colunas_usar,
                    chunksize=100_000, low_memory=False,
                    dtype={'SG_UF': str, 'ID_MUNICIP': str}
                )
        else:
            df_iter = pd.read_csv(
                io.BytesIO(response.content),
                sep=",", encoding="latin1",
                usecols=lambda x: x in colunas_usar,
                chunksize=100_000, low_memory=False,
                dtype={'SG_UF': str, 'ID_MUNICIP': str}
            )
    except Exception as e:
        raise RuntimeError(f"Erro ao processar arquivo de {arbovirose} {ano}: {e}")

    chunks = list(df_iter)
    if not chunks:
        df_vazio = pd.DataFrame(columns=colunas_usar + ['ANO_MES', 'REGIAO', 'FAIXA_ETARIA', 'EM_INVESTIGACAO'])
        CacheManagerArboviroses.salvar(arbovirose, ano, df_vazio)
        return df_vazio, False

    df = pd.concat(chunks, ignore_index=True)
    logger.info(f"Dados brutos de {arbovirose} carregados: {len(df):,} registros")

    if arbovirose.lower() == "dengue":
        df = DataCleaner.pipeline_limpeza_dengue(df, ano)
    elif arbovirose.lower() == "zika":
        df = DataCleaner.pipeline_limpeza_zika(df, ano)
    elif arbovirose.lower() == "chikungunya":
        df = DataCleaner.pipeline_limpeza_chikungunya(df, ano)

    if usar_cache and not df.empty:
        CacheManagerArboviroses.salvar(arbovirose, ano, df)
        logger.info(f"Dados de {arbovirose.upper()} {ano} processados e salvos no cache.")

    return df, False

def gerar_relatorio_qualidade(df: pd.DataFrame) -> Dict:
    """
    Gera relatório de qualidade dos dados de arboviroses
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        Dicionário com relatório de qualidade
    """
    if df.empty:
        return {}
    
    casos_investigacao = 0
    if 'EM_INVESTIGACAO' in df.columns:
        casos_investigacao = df['EM_INVESTIGACAO'].sum()
    
    data_min = 'N/A'
    data_max = 'N/A'
    if 'DT_NOTIFIC' in df.columns and pd.api.types.is_datetime64_any_dtype(df['DT_NOTIFIC']):
        datas_validas = df['DT_NOTIFIC'].notna()
        if datas_validas.any():
            data_min = df[datas_validas]['DT_NOTIFIC'].min().strftime('%Y-%m-%d')
            data_max = df[datas_validas]['DT_NOTIFIC'].max().strftime('%Y-%m-%d')
    
    relatorio = {
        'total_registros': len(df),
        'casos_investigacao': casos_investigacao,
        'periodo_cobertura': {
            'data_min': data_min,
            'data_max': data_max
        },
        'distribuicao_regiao': df['REGIAO'].value_counts().to_dict() if 'REGIAO' in df.columns else {},
        'completude_campos': {
            col: {
                'total': len(df),
                'nao_nulos': df[col].notna().sum(),
                'percentual': (df[col].notna().sum() / len(df)) * 100
            } for col in df.columns
        }
    }
    
    if 'CS_SEXO' in df.columns:
        relatorio['distribuicao_sexo'] = df['CS_SEXO'].value_counts().to_dict()
    
    return relatorio

def agrupar_casos_por_mes(df: pd.DataFrame, ano: int) -> pd.DataFrame:
    """
    Agrupa casos por região e mês, garantindo todos os meses do ano
    
    Args:
        df: DataFrame com dados processados
        ano: Ano para estrutura de meses
        
    Returns:
        DataFrame com casos agrupados por mês e região
    """
    if df.empty or 'REGIAO' not in df.columns:
        return pd.DataFrame(columns=['REGIAO','ANO_MES','CASOS','MES_FORMATADO'])
    
    if 'ANO_MES' not in df.columns:
        logger.warning("Coluna ANO_MES não encontrada - criando a partir de DT_NOTIFIC")
        if 'DT_NOTIFIC' in df.columns:
            df['ANO_MES'] = df['DT_NOTIFIC'].dt.to_period('M')
        else:
            logger.error("Não é possível criar ANO_MES: DT_NOTIFIC não encontrada")
            return pd.DataFrame(columns=['REGIAO','ANO_MES','CASOS','MES_FORMATADO'])
    
    df_agrupado = df.groupby(['REGIAO', 'ANO_MES']).size().reset_index(name='CASOS')
    
    meses_pt = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 
                'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    
    regioes = df['REGIAO'].unique()
    
    meses_completos = []
    for regiao in regioes:
        for mes in range(1, 13):
            periodo = pd.Period(year=ano, month=mes, freq='M')
            meses_completos.append({
                'REGIAO': regiao,
                'ANO_MES': periodo,
                'MES_FORMATADO': f"{meses_pt[mes-1]}/{str(ano)[-2:]}",
                'MES_NUMERO': mes
            })
    
    df_completo = pd.DataFrame(meses_completos)
    
    if not df_agrupado.empty:
        df_completo = df_completo.merge(
            df_agrupado[['REGIAO', 'ANO_MES', 'CASOS']], 
            on=['REGIAO', 'ANO_MES'], 
            how='left'
        )
        df_completo['CASOS'] = df_completo['CASOS'].fillna(0)
    else:
        df_completo['CASOS'] = 0
    
    df_completo = df_completo.sort_values(['REGIAO', 'MES_NUMERO'])
    
    df_completo = df_completo.drop('MES_NUMERO', axis=1)
    
    logger.info(f"Gráfico gerado com {len(df_completo)} registros (12 meses × {len(regioes)} regiões)")
    
    return df_completo

def limpar_cache_arboviroses():
    """Limpa todo o cache de dados de arboviroses"""
    try:
        if os.path.exists(ARBOVIROSES_CACHE_DIR):
            for arquivo in os.listdir(ARBOVIROSES_CACHE_DIR):
                os.remove(os.path.join(ARBOVIROSES_CACHE_DIR, arquivo))
            logger.info("Cache de arboviroses limpo com sucesso!")
        else:
            logger.info("Diretório de cache não existe")
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {e}")