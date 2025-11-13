"""
Módulo: utils_climate.py
Descrição: Processamento de dados climáticos do INMET para análise de arboviroses
Funções para download, limpeza e análise de dados climáticos por região
Desenvolvido para: Trabalho de Conclusão de Curso (TCC) - Análise de Dados e Correlação Climática com Arboviroses nas cinco regiões do Brasil
Autor: Eloy Cardozo Augusto
Código: 836463
"""

import time
import traceback
import pandas as pd
import requests
import zipfile
import io
import numpy as np
import logging
import os
import pickle
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL_BASE = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"

MAPEAMENTO_REGIOES = {
    'CO_': 'Centro-Oeste',
    'N_': 'Norte', 
    'NE_': 'Nordeste',
    'SE_': 'Sudeste',
    'S_': 'Sul'
}

VARIAVEIS_CLIMATICAS = {
    'Data': 'data',
    'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitacao_mm',
    'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperatura_c',
    'UMIDADE RELATIVA DO AR, HORARIA (%)': 'umidade_percentual'
}

CACHE_DIR = "Dados"
CLIMA_CACHE_DIR = os.path.join(CACHE_DIR, "clima")

class CacheManagerClima:
    """
    Gerencia cache de dados climáticos com TTL (Time To Live)
    """
    
    CACHE_TTL_HORAS = 24
    CACHE_TTL_SEGUNDOS = CACHE_TTL_HORAS * 3600
    
    @staticmethod
    def _criar_diretorios():
        """Cria diretórios de cache se não existirem"""
        os.makedirs(CLIMA_CACHE_DIR, exist_ok=True)
        logger.info(f"Diretório de cache verificado/criado: {CLIMA_CACHE_DIR}")
    
    @staticmethod
    def _gerar_nome_arquivo(ano: int) -> str:
        """Gera nome do arquivo de cache baseado no ano"""
        return f"clima_{ano}.pkl"
    
    @staticmethod
    def _caminho_arquivo(ano: int) -> str:
        """Retorna caminho completo do arquivo de cache"""
        CacheManagerClima._criar_diretorios()
        nome_arquivo = CacheManagerClima._gerar_nome_arquivo(ano)
        return os.path.join(CLIMA_CACHE_DIR, nome_arquivo)
    
    @staticmethod
    def salvar(ano: int, dados: pd.DataFrame) -> bool:
        """
        Salva dados no cache
        
        Args:
            ano: Ano dos dados
            dados: DataFrame a ser salvo
            
        Returns:
            True se salvou com sucesso
        """
        try:
            caminho = CacheManagerClima._caminho_arquivo(ano)
            
            with open(caminho, 'wb') as f:
                pickle.dump(dados, f)
            
            logger.info(f"Dados climáticos {ano} salvos em cache: {caminho}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar cache climático {ano}: {e}")
            return False
    
    @staticmethod
    def carregar(ano: int) -> Optional[pd.DataFrame]:
        """
        Carrega dados do cache se existirem e estiverem válidos
        
        Args:
            ano: Ano dos dados
            
        Returns:
            DataFrame carregado ou None se não existir ou expirado
        """
        try:
            caminho = CacheManagerClima._caminho_arquivo(ano)
            
            if not os.path.exists(caminho):
                logger.info(f"Cache climático não encontrado: {caminho}")
                return None
            
            if not CacheManagerClima._cache_esta_valido(caminho, ano):
                logger.info(f"Cache climático expirado, removendo: {caminho}")
                os.remove(caminho)
                return None
            
            with open(caminho, 'rb') as f:
                dados = pickle.load(f)
            
            logger.info(f"Dados climáticos {ano} carregados do cache: {caminho}")
            return dados
            
        except Exception as e:
            logger.error(f"Erro ao carregar cache climático {ano}: {e}")
            return None
    
    @staticmethod
    def existe(ano: int) -> bool:
        """
        Verifica se dados existem no cache
        
        Args:
            ano: Ano dos dados
            
        Returns:
            True se existe no cache
        """
        caminho = CacheManagerClima._caminho_arquivo(ano)
        if not os.path.exists(caminho):
            return False
        
        return CacheManagerClima._cache_esta_valido(caminho, ano)
    
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
            if idade_cache_segundos > CacheManagerClima.CACHE_TTL_SEGUNDOS:
                logger.info(f"Cache climático expirado para {ano} (atual): {idade_cache_segundos/3600:.1f}h > {CacheManagerClima.CACHE_TTL_HORAS}h")
                return False
            else:
                logger.info(f"Cache climático válido para {ano} (atual): {idade_cache_segundos/3600:.1f}h")
                return True
        else:
            logger.info(f"Cache climático válido para {ano} (histórico)")
            return True

class ClimateDataProcessor:
    """
    Processa dados climáticos do INMET - download, limpeza e consolidação
    """
    
    @staticmethod
    def baixar_dados_inmet(ano: int) -> zipfile.ZipFile:
        """
        Baixa dados do INMET para o ano especificado
        
        Args:
            ano: Ano dos dados a serem baixados
            
        Returns:
            Arquivo ZIP com dados brutos
        """
        url = URL_BASE.format(ano=ano)
        
        try:
            logger.info(f"Baixando dados INMET para {ano}...")
            
            session = requests.Session()
            session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))
            
            response = session.get(url, timeout=180, stream=True)
            response.raise_for_status()
            
            content_length = response.headers.get('Content-Length')
            if content_length:
                logger.info(f"Tamanho do arquivo: {int(content_length) / (1024*1024):.2f} MB")
            
            content = response.content
            if not content:
                raise RuntimeError("Conteúdo vazio recebido do servidor")
                
            zip_file = zipfile.ZipFile(io.BytesIO(content))
            csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
            logger.info(f"Encontrados {len(csv_files)} arquivos CSV")
            
            return zip_file
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de conexão ao baixar dados INMET {ano}: {e}")
            raise RuntimeError(f"Erro de conexão ao baixar dados INMET {ano}. Tente novamente.")
        except zipfile.BadZipFile:
            logger.error(f"Arquivo ZIP corrompido para {ano}")
            raise RuntimeError(f"Arquivo ZIP corrompido para {ano}. O servidor pode estar com problemas.")
        except Exception as e:
            logger.error(f"Erro inesperado ao processar arquivo ZIP: {e}")
            raise
    
    @staticmethod
    def extrair_regiao(nome_arquivo: str) -> str:
        """
        Extrai região do Brasil baseado no nome do arquivo
        
        Args:
            nome_arquivo: Nome do arquivo CSV
            
        Returns:
            Nome da região
        """
        for prefixo, regiao in MAPEAMENTO_REGIOES.items():
            if prefixo in nome_arquivo:
                return regiao
        return 'Outra'
    
    @staticmethod
    def processar_arquivo_climatico(nome_arquivo: str, conteudo: str, ano_alvo: int) -> Optional[pd.DataFrame]:
        """
        Processa arquivo CSV individual do INMET com suporte para formatos problemáticos anteriores a 2019.
        
        Args:
            nome_arquivo (str): O nome do arquivo que está sendo processado.
            conteudo (str): O conteúdo textual (CSV) do arquivo.
            ano_alvo (int): O ano de referência para filtrar os dados.
            
        Returns:
            Optional[pd.DataFrame]: Um DataFrame agregado mensalmente ou None se o processamento falhar.
        """
        try:
            from io import StringIO
            
            formato_antigo = ano_alvo < 2019
            
            if formato_antigo:
                try:
                    df = pd.read_csv(
                        StringIO(conteudo),
                        sep=';',
                        skiprows=8,
                        decimal=',',
                        encoding='latin-1',
                        on_bad_lines='skip',
                        header=0,
                        low_memory=False
                    )
                    
                    if len(df.columns) == 1 and any(';' in str(val) for val in df.iloc[:, 0].dropna().head()):
                        logger.info(f"Dividindo colunas concatenadas: {nome_arquivo}")
                        
                        dados_divididos = df.iloc[:, 0].str.split(';', expand=True)
                        
                        if not dados_divididos.empty:
                            novos_cabecalhos = dados_divididos.iloc[0].str.strip()
                            dados_divididos = dados_divididos[1:]
                            dados_divididos.columns = novos_cabecalhos
                            df = dados_divididos.reset_index(drop=True)
                            
                except Exception as e:
                    logger.warning(f"Tentando abordagem alternativa para: {nome_arquivo}")
                    try:
                        df_temp = pd.read_csv(
                            StringIO(conteudo),
                            sep='§',
                            skiprows=8,
                            encoding='latin-1',
                            header=None,
                            names=['dados_concatenados']
                        )
                        
                        if not df_temp.empty and 'dados_concatenados' in df_temp.columns:
                            dados_divididos = df_temp['dados_concatenados'].str.split(';', expand=True)
                            if not dados_divididos.empty:
                                novos_cabecalhos = dados_divididos.iloc[0].str.strip()
                                dados_divididos = dados_divididos[1:]
                                dados_divididos.columns = novos_cabecalhos
                                df = dados_divididos.reset_index(drop=True)
                    except Exception:
                        return None
            else:
                df = pd.read_csv(
                    StringIO(conteudo),
                    sep=';',
                    skiprows=8,
                    decimal=',',
                    encoding='latin-1',
                    on_bad_lines='skip',
                    header=0,
                    low_memory=False
                )
            
            mapeamento_colunas = {}
            colunas_detectadas = list(df.columns)
            
            palavras_chave = {
                'data': ['DATA', 'Data', 'data', 'DT_MEDICAO'],
                'precipitacao': ['PRECIPITAÇÃO', 'PRECIPITACAO', 'Precipitacao', 'precipitacao', 'CHUVA', 'Chuva'],
                'temperatura': ['TEMPERATURA', 'Temperatura', 'temperatura', 'BULBO SECO', 'BULBO_SECO', 'TEMPERATURA DO AR'],
                'umidade': ['UMIDADE RELATIVA DO AR', 'UMIDADE_RELATIVA', 'UMIDADE RELATIVA', 'UMIDADE', 'Umidade', 'umidade', 'RELATIVA']
            }
            
            for col_novo, palavras in palavras_chave.items():
                for col_original in colunas_detectadas:
                    col_original_str = str(col_original)
                    if any(palavra.upper() in col_original_str.upper() for palavra in palavras):
                        if col_novo == 'umidade' and 'HORARIA' in col_original_str.upper():
                            mapeamento_colunas[col_original] = col_novo
                            break
                        elif col_novo not in [k for k, v in mapeamento_colunas.items() if v == col_novo]:
                            mapeamento_colunas[col_original] = col_novo
                            break
            
            if not mapeamento_colunas and formato_antigo:
                mapeamentos_tentativas = [
                    {
                        'DATA (YYYY-MM-DD)': 'data',
                        'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitacao',
                        'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperatura',
                    'UMIDADE RELATIVA DO AR, HORARIA (%)': 'umidade'
                    },
                    {
                        'Data': 'data',
                        'Precipitacao': 'precipitacao',
                        'TempBulboSeco': 'temperatura',
                        'UmidadeRelativa': 'umidade'
                    }
                ]
                
                for mapeamento in mapeamentos_tentativas:
                    for col_original, col_novo in mapeamento.items():
                        if col_original in colunas_detectadas:
                            mapeamento_colunas[col_original] = col_novo
            
            if not mapeamento_colunas:
                return None
                
            df = df.rename(columns=mapeamento_colunas)
            
            colunas_relevantes = ['data', 'precipitacao', 'temperatura', 'umidade']
            colunas_existentes = [col for col in colunas_relevantes if col in df.columns]
            
            if 'data' not in colunas_existentes:
                return None
                
            df = df[colunas_existentes]
            
            if formato_antigo:
                df['data'] = pd.to_datetime(df['data'], errors='coerce', format='%Y-%m-%d')
                if df['data'].isnull().all():
                    df['data'] = pd.to_datetime(df['data'], errors='coerce', dayfirst=True)
                if df['data'].isnull().all():
                    df['data'] = pd.to_datetime(df['data'], errors='coerce', format='%d/%m/%Y')
            else:
                df['data'] = pd.to_datetime(df['data'], errors='coerce', dayfirst=False)
            
            df = df.dropna(subset=['data'])
            
            df = df[df['data'].dt.year == ano_alvo]
            
            if df.empty:
                return None
                
            for coluna in ['precipitacao', 'temperatura', 'umidade']:
                if coluna in df.columns:
                    serie_limpa = df[coluna].astype(str)
                    serie_limpa = serie_limpa.str.replace(r'[^\d\.,\-]', '', regex=True)
                    serie_limpa = serie_limpa.str.replace(',', '.')
                    df[coluna] = pd.to_numeric(serie_limpa, errors='coerce')
                    
                    if coluna == 'temperatura':
                        df = df[(df[coluna] >= -50) & (df[coluna] <= 50)]
                    elif coluna == 'precipitacao':
                        df = df[(df[coluna] >= 0) & (df[coluna] <= 500)]
                    elif coluna == 'umidade':
                        df = df[(df[coluna] >= 0) & (df[coluna] <= 100)]
            
            if df.empty:
                return None
                
            df['ano_mes'] = df['data'].dt.to_period('M')
            
            agg_dict = {}
            if 'precipitacao' in df.columns:
                agg_dict['precipitacao'] = 'sum'
            if 'temperatura' in df.columns:
                agg_dict['temperatura'] = 'mean'
            if 'umidade' in df.columns:
                agg_dict['umidade'] = 'mean'
                
            if not agg_dict:
                return None
                
            df_mensal = df.groupby('ano_mes').agg(agg_dict).reset_index()
            
            df_mensal = df_mensal.rename(columns={
                'precipitacao': 'precipitacao_mm',
                'temperatura': 'temperatura_c',
                'umidade': 'umidade_percentual'
            })
            
            regiao = ClimateDataProcessor.extrair_regiao(nome_arquivo)
            df_mensal['regiao'] = regiao
            df_mensal['estacao'] = nome_arquivo
            
            return df_mensal

        except Exception as e:
            logger.error(f"Erro ao processar {nome_arquivo}: {e}")
            return None
    
    @staticmethod
    def processar_dados_climaticos(ano: int) -> pd.DataFrame:
        """
        Processa todos os dados climáticos do INMET para um ano específico
        
        Args:
            ano: Ano dos dados
            
        Returns:
            DataFrame consolidado com dados mensais por região
        """
        try:
            zip_file = ClimateDataProcessor.baixar_dados_inmet(ano)
            csv_files = [f for f in zip_file.namelist() if f.endswith('.CSV')]
            
            logger.info(f"Iniciando processamento de {len(csv_files)} arquivos para {ano}")
            
            dados_todos = []
            contadores = {'sucesso': 0, 'erro': 0}
            
            for i, nome_arquivo in enumerate(csv_files):
                if i % 50 == 0:
                    logger.info(f"Progresso: {i}/{len(csv_files)}")
                
                try:
                    with zip_file.open(nome_arquivo) as f:
                        conteudo = f.read().decode('latin-1')
                    
                    df_processado = ClimateDataProcessor.processar_arquivo_climatico(
                        nome_arquivo, conteudo, ano
                    )
                    
                    if df_processado is not None and not df_processado.empty:
                        dados_todos.append(df_processado)
                        contadores['sucesso'] += 1
                    else:
                        contadores['erro'] += 1
                        
                except Exception as e:
                    logger.error(f"Erro fatal em {nome_arquivo}: {e}")
                    contadores['erro'] += 1
            
            logger.info(f"Processamento concluído: {contadores['sucesso']} sucessos, {contadores['erro']} erros")
            
            if not dados_todos:
                raise RuntimeError("Nenhum arquivo foi processado com sucesso")
            
            df_final = pd.concat(dados_todos, ignore_index=True)
            
            colunas_numericas = [col for col in df_final.columns 
                            if col not in ['ano_mes', 'regiao', 'estacao']]
            
            df_consolidado = df_final.groupby(['ano_mes', 'regiao']).agg({
                col: 'mean' for col in colunas_numericas
            }).reset_index()
            
            df_consolidado['data'] = pd.to_datetime(df_consolidado['ano_mes'].astype(str))
            df_consolidado = df_consolidado.drop('ano_mes', axis=1)
            
            colunas_ordenadas = ['data', 'regiao'] + colunas_numericas
            df_consolidado = df_consolidado[colunas_ordenadas]
            
            logger.info(f"Dados consolidados: {len(df_consolidado)} registros")
            
            todos_meses = pd.date_range(start=f'{ano}-01-01', end=f'{ano}-12-31', freq='MS')
            todas_regioes = df_consolidado['regiao'].unique()
            
            logger.info(f"Criando estrutura completa: {len(todos_meses)} meses × {len(todas_regioes)} regiões")
            
            estrutura_completa = []
            for data in todos_meses:
                for regiao in todas_regioes:
                    estrutura_completa.append({
                        'data': data,
                        'regiao': regiao
                    })
            
            df_estrutura = pd.DataFrame(estrutura_completa)
            
            df_final_completo = df_estrutura.merge(
                df_consolidado, 
                on=['data', 'regiao'], 
                how='left'
            )
            
            colunas_preencher = [col for col in colunas_numericas if col in df_final_completo.columns]
            
            for coluna in colunas_preencher:
                df_final_completo = df_final_completo.sort_values(['regiao', 'data'])
                
                for coluna in colunas_preencher:
                    df_final_completo = df_final_completo.sort_values(['regiao', 'data'])
                    
                    for regiao in df_final_completo['regiao'].unique():
                        mask = df_final_completo['regiao'] == regiao
                        dados_regiao = df_final_completo.loc[mask, coluna]
                        
                        if dados_regiao.notna().sum() >= 6:
                            df_final_completo.loc[mask, coluna] = dados_regiao.interpolate(
                                method='linear', limit=2, limit_direction='forward'
                            )
                    
                    missing_apos = df_final_completo[coluna].isnull().sum()
                    if missing_apos > len(df_final_completo) * 0.3:
                        logger.warning(f"Muitos dados missing em {coluna} após interpolação")
                
                if df_final_completo[coluna].isnull().any():
                    media_geral = df_final_completo[coluna].mean()
                    df_final_completo[coluna] = df_final_completo[coluna].fillna(media_geral)
            
            meses_unicos = df_final_completo['data'].dt.month.unique()
            logger.info(f"Meses com dados após preenchimento: {sorted(meses_unicos)}")
            
            if len(meses_unicos) == 12:
                logger.info("TODOS OS 12 MESES PREENCHIDOS COM SUCESSO!")
            else:
                logger.warning(f"Apenas {len(meses_unicos)} meses preenchidos")
            
            df_final_completo = df_final_completo.sort_values(['regiao', 'data']).reset_index(drop=True)
            
            logger.info(f"Dados climáticos COMPLETOS: {len(df_final_completo)} registros "
                    f"({len(todos_meses)} meses × {len(todas_regioes)} regiões)")
            
            diagnostico = ClimateDataProcessor.diagnosticar_qualidade_dados(df_final_completo)
            
            return df_final_completo
            
        except Exception as e:
            logger.error(f"Erro no processamento geral: {e}")
            raise
    
    @staticmethod
    def tratar_dados_climaticos(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica tratamento de qualidade aos dados climáticos
        
        Args:
            df: DataFrame com dados climáticos
            
        Returns:
            DataFrame tratado
        """
        if df.empty:
            return df
            
        df_tratado = df.copy()
        
        variaveis_numericas = ['precipitacao_mm', 'temperatura_c', 'umidade_percentual',
                              'pressao_max_mb', 'radiacao_kjm2', 'vento_velocidade_ms']
        variaveis_existentes = [var for var in variaveis_numericas if var in df_tratado.columns]
        
        for var in variaveis_existentes:
            missing_antes = df_tratado[var].isnull().sum()
            
            if missing_antes > 0:
                df_tratado[var] = df_tratado.groupby(['regiao', df_tratado['data'].dt.month])[var].transform(
                    lambda x: x.fillna(x.median())
                )
                
                df_tratado[var] = df_tratado.groupby('regiao')[var].transform(
                    lambda x: x.fillna(x.median())
                )
        
        for var in variaveis_existentes:
            Q1 = df_tratado[var].quantile(0.05)
            Q3 = df_tratado[var].quantile(0.95)
            df_tratado[var] = np.clip(df_tratado[var], Q1, Q3)
        
        logger.info(f"Dados tratados: {len(df_tratado)} registros")
        return df_tratado
    
    @staticmethod
    def gerar_relatorio_qualidade(df: pd.DataFrame) -> Dict:
        """
        Gera relatório de qualidade dos dados climáticos
        
        Args:
            df: DataFrame com dados climáticos
            
        Returns:
            Dicionário com relatório de qualidade
        """
        if df.empty:
            return {}
        
        relatorio = {
            'total_registros': len(df),
            'periodo': {
                'inicio': df['data'].min().strftime('%Y-%m-%d'),
                'fim': df['data'].max().strftime('%Y-%m-%d')
            },
            'regioes': df['regiao'].unique().tolist(),
            'dados_faltantes': {},
            'estatisticas_regiao': {}
        }
        
        for coluna in df.columns:
            if coluna != 'data':
                missing_count = df[coluna].isnull().sum()
                missing_percent = (missing_count / len(df)) * 100
                relatorio['dados_faltantes'][coluna] = {
                    'count': missing_count,
                    'percentual': round(missing_percent, 2)
                }
        
        for regiao in df['regiao'].unique():
            df_regiao = df[df['regiao'] == regiao]
            stats_regiao = {}
            
            for coluna in df.columns:
                if coluna not in ['data', 'regiao'] and pd.api.types.is_numeric_dtype(df[coluna]):
                    stats_regiao[coluna] = {
                        'media': round(df_regiao[coluna].mean(), 2),
                        'mediana': round(df_regiao[coluna].median(), 2),
                        'min': round(df_regiao[coluna].min(), 2),
                        'max': round(df_regiao[coluna].max(), 2),
                        'desvio_padrao': round(df_regiao[coluna].std(), 2)
                    }
            
            relatorio['estatisticas_regiao'][regiao] = stats_regiao
        
        return relatorio
    
    @staticmethod
    def diagnosticar_qualidade_dados(df: pd.DataFrame) -> Dict:
        """
        Diagnóstico detalhado da qualidade dos dados climáticos
        """
        logger.info("INICIANDO DIAGNÓSTICO DETALHADO DOS DADOS CLIMÁTICOS...")
        
        diagnostico = {
            'estrutura_geral': {
                'total_registros': len(df),
                'periodo_inicio': df['data'].min(),
                'periodo_fim': df['data'].max(),
                'regioes': df['regiao'].unique().tolist()
            },
            'qualidade_por_regiao': {},
            'problemas_detectados': []
        }
        
        for regiao in df['regiao'].unique():
            df_regiao = df[df['regiao'] == regiao]
            
            diagnostico_regiao = {
                'total_meses': len(df_regiao),
                'meses_completos': 0,
                'variaveis_analise': {}
            }
            
            for variavel in ['precipitacao_mm', 'temperatura_c', 'umidade_percentual']:
                if variavel in df_regiao.columns:
                    dados_variavel = df_regiao[variavel]
                    
                    stats = {
                        'media': round(dados_variavel.mean(), 2),
                        'mediana': round(dados_variavel.median(), 2),
                        'min': round(dados_variavel.min(), 2),
                        'max': round(dados_variavel.max(), 2),
                        'missing': dados_variavel.isnull().sum(),
                        'missing_percent': round((dados_variavel.isnull().sum() / len(dados_variavel)) * 100, 1),
                        'zeros': (dados_variavel == 0).sum(),
                        'zeros_percent': round(((dados_variavel == 0).sum() / len(dados_variavel)) * 100, 1)
                    }
                    
                    diagnostico_regiao['variaveis_analise'][variavel] = stats
                    
                    if stats['missing_percent'] > 20:
                        diagnostico['problemas_detectados'].append(
                            f"{regiao} - {variavel}: {stats['missing_percent']}% dados missing"
                        )
                    if stats['zeros_percent'] > 50 and variavel != 'precipitacao_mm':
                        diagnostico['problemas_detectados'].append(
                            f"{regiao} - {variavel}: {stats['zeros_percent']}% valores zero"
                        )
                    if stats['max'] - stats['min'] < 1 and variavel != 'umidade_percentual':
                        diagnostico['problemas_detectados'].append(
                            f"{regiao} - {variavel}: Pouca variação ({stats['min']} a {stats['max']})"
                        )
            
            diagnostico['qualidade_por_regiao'][regiao] = diagnostico_regiao
        
        logger.info("DIAGNÓSTICO CLIMÁTICO:")
        logger.info(f"Total de registros: {diagnostico['estrutura_geral']['total_registros']}")
        
        for regiao, info in diagnostico['qualidade_por_regiao'].items():
            logger.info(f"{regiao}: {info['total_meses']} meses")
            for variavel, stats in info['variaveis_analise'].items():
                logger.info(f"   {variavel}: média={stats['media']}, missing={stats['missing_percent']}%")
        
        for problema in diagnostico['problemas_detectados']:
            logger.warning(problema)
        
        return diagnostico


def baixar_dados_climaticos(ano: int, usar_cache: bool = True) -> Tuple[pd.DataFrame, bool]:
    """
    Função principal para baixar e processar dados climáticos do INMET
    
    Args:
        ano: Ano dos dados
        usar_cache: Usar cache para melhor performance
        
    Returns:
        Tuple[DataFrame processado, True se veio do cache]
    """
    try:
        if usar_cache and CacheManagerClima.existe(ano):
            dados_cache = CacheManagerClima.carregar(ano)
            if dados_cache is not None and not dados_cache.empty:
                logger.info(f"Dados climáticos {ano} carregados do cache local.")
                return dados_cache, True

        logger.info(f"Baixando e processando dados climáticos para {ano}...")
        df = ClimateDataProcessor.processar_dados_climaticos(ano)

        df_tratado = ClimateDataProcessor.tratar_dados_climaticos(df)

        if usar_cache and not df_tratado.empty:
            CacheManagerClima.salvar(ano, df_tratado)
            logger.info(f"Dados climáticos {ano} processados e salvos no cache.")

        return df_tratado, False

    except Exception as e:
        logger.error(f"Erro ao baixar/processar dados climáticos {ano}: {e}")
        raise


def tratar_dados_climaticos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Função principal para tratamento de dados climáticos
    
    Args:
        df: DataFrame com dados climáticos brutos
        
    Returns:
        DataFrame com dados tratados
    """
    return ClimateDataProcessor.tratar_dados_climaticos(df)


def gerar_relatorio_clima(df: pd.DataFrame) -> Dict:
    """
    Função principal para gerar relatório de qualidade
    
    Args:
        df: DataFrame com dados climáticos
        
    Returns:
        Relatório de qualidade
    """
    return ClimateDataProcessor.gerar_relatorio_qualidade(df)


def limpar_cache_clima():
    """Limpa todo o cache de dados climáticos"""
    try:
        if os.path.exists(CLIMA_CACHE_DIR):
            for arquivo in os.listdir(CLIMA_CACHE_DIR):
                os.remove(os.path.join(CLIMA_CACHE_DIR, arquivo))
            logger.info("Cache de dados climáticos limpo com sucesso!")
        else:
            logger.info("Diretório de cache climático não existe")
    except Exception as e:
        logger.error(f"Erro ao limpar cache climático: {e}")