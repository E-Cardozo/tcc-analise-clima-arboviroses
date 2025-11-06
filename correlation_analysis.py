"""
Módulo: correlation_analysis.py
Descrição: Análise de correlação entre dados epidemiológicos e climáticos
Desenvolvido para: Trabalho de Conclusão de Curso (TCC) - Análise de Dados e Correlação Climática com Arboviroses nas cinco regiões do Brasil
Funções para correlacionar casos de arboviroses com variáveis climáticas
Autor: Eloy Cardozo Augusto
Código: 836463
"""

import time
import pandas as pd
import numpy as np
import os
import pickle
from scipy import stats
import logging
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = "Dados"
CORRELACAO_CACHE_DIR = os.path.join(CACHE_DIR, "correlacao")

class CacheManagerCorrelacao:
    """
    Gerencia cache de análises de correlação com TTL (Time To Live)
    """
    
    CACHE_TTL_HORAS = 24
    CACHE_TTL_SEGUNDOS = CACHE_TTL_HORAS * 3600
    
    @staticmethod
    def _criar_diretorios():
        """Cria diretórios de cache se não existirem"""
        os.makedirs(CORRELACAO_CACHE_DIR, exist_ok=True)

    @staticmethod
    def _gerar_nome_arquivo(arbovirose: str, ano: int, variavel_climatica: Optional[str] = None) -> str:
        """
        Gera nome do arquivo de cache incluindo variável climática
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            variavel_climatica: Variável climática específica
            
        Returns:
            Nome do arquivo de cache
        """
        if variavel_climatica:
            return f"{arbovirose.lower()}_{variavel_climatica}_{ano}.pkl"
        return f"{arbovirose.lower()}_clima_{ano}.pkl"
    
    @staticmethod
    def _caminho_arquivo(arbovirose: str, ano: int, variavel_climatica: Optional[str] = None) -> str:
        """Retorna caminho completo do arquivo de cache"""
        CacheManagerCorrelacao._criar_diretorios()
        nome_arquivo = CacheManagerCorrelacao._gerar_nome_arquivo(arbovirose, ano, variavel_climatica)
        return os.path.join(CORRELACAO_CACHE_DIR, nome_arquivo)
    
    @staticmethod
    def salvar(arbovirose: str, ano: int, dados: Dict, variavel_climatica: Optional[str] = None) -> bool:
        """
        Salva análise de correlação no cache
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            dados: Dicionário com resultados da análise
            variavel_climatica: Variável climática específica
            
        Returns:
            True se salvou com sucesso
        """
        try:
            caminho = CacheManagerCorrelacao._caminho_arquivo(arbovirose, ano, variavel_climatica)
            with open(caminho, 'wb') as f:
                pickle.dump(dados, f)
            logger.info(f"Análise de correlação salva em cache: {caminho}")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar cache de correlação: {e}")
            return False
    
    @staticmethod
    def carregar(arbovirose: str, ano: int, variavel_climatica: Optional[str] = None) -> Optional[Dict]:
        """
        Carrega análise de correlação do cache se existir e estiver válida
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            variavel_climatica: Variável climática específica
            
        Returns:
            Dicionário com dados carregados ou None
        """
        try:
            caminho = CacheManagerCorrelacao._caminho_arquivo(arbovirose, ano, variavel_climatica)
            if not os.path.exists(caminho):
                logger.info(f"Cache de correlação não encontrado: {caminho}")
                return None
            
            if not CacheManagerCorrelacao._cache_esta_valido(caminho, ano):
                logger.info(f"Cache correlação expirado, removendo: {caminho}")
                os.remove(caminho)
                return None
            
            with open(caminho, 'rb') as f:
                dados = pickle.load(f)
            logger.info(f"Análise de correlação carregada do cache: {caminho}")
            return dados
        except Exception as e:
            logger.error(f"Erro ao carregar cache de correlação: {e}")
            return None
    
    @staticmethod
    def existe(arbovirose: str, ano: int, variavel_climatica: Optional[str] = None) -> bool:
        """
        Verifica se análise existe no cache e está válida
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            variavel_climatica: Variável climática específica
            
        Returns:
            True se existe no cache e é válido
        """
        caminho = CacheManagerCorrelacao._caminho_arquivo(arbovirose, ano, variavel_climatica)
        if not os.path.exists(caminho):
            return False
        
        return CacheManagerCorrelacao._cache_esta_valido(caminho, ano)
    
    @staticmethod
    def limpar_variavel_especifica(arbovirose: str, ano: int, variavel_climatica: str):
        """
        Remove cache de uma variável climática específica
        
        Args:
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            variavel_climatica: Variável climática a ser removida
            
        Returns:
            True se removeu com sucesso
        """
        try:
            caminho = CacheManagerCorrelacao._caminho_arquivo(arbovirose, ano, variavel_climatica)
            if os.path.exists(caminho):
                os.remove(caminho)
                logger.info(f"Cache de {variavel_climatica} removido: {caminho}")
                return True
            return False
        except Exception as e:
            logger.error(f"Erro ao remover cache específico: {e}")
            return False
        
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
            if idade_cache_segundos > CacheManagerCorrelacao.CACHE_TTL_SEGUNDOS:
                logger.info(f"Cache correlação expirado para {ano} (atual): {idade_cache_segundos/3600:.1f}h > {CacheManagerCorrelacao.CACHE_TTL_HORAS}h")
                return False
            else:
                logger.info(f"Cache correlação válido para {ano} (atual): {idade_cache_segundos/3600:.1f}h")
                return True
        else:
            logger.info(f"Cache correlação válido para {ano} (histórico)")
            return True

class CorrelationAnalyzer:
    """
    Analisa correlação entre dados epidemiológicos e climáticos com defasagem temporal
    """
    
    @staticmethod
    def preparar_dados_correlacao(df_arboviroses: pd.DataFrame, df_clima: pd.DataFrame, 
                                arbovirose: str, ano: int, defasagem_meses: int = 1) -> pd.DataFrame:
        """
        Prepara dados para análise de correlação unindo datasets com defasagem temporal
        
        Args:
            df_arboviroses: DataFrame com dados de arboviroses
            df_clima: DataFrame com dados climáticos
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            defasagem_meses: Defasagem temporal em meses
            
        Returns:
            DataFrame preparado para análise de correlação
        """
        try:
            logger.info(f"Preparando dados para correlação com defasagem de {defasagem_meses} mês(es)")
            
            MESES_PT_BR = {
                1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
                7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
            }
            
            if 'ANO_MES' not in df_arboviroses.columns and 'DT_NOTIFIC' in df_arboviroses.columns:
                df_arboviroses['ANO_MES'] = df_arboviroses['DT_NOTIFIC'].dt.to_period('M')
            
            casos_por_mes_regiao = df_arboviroses.groupby(['REGIAO', 'ANO_MES']).size().reset_index(name='casos_arbovirose')
            casos_por_mes_regiao['data_arbovirose'] = casos_por_mes_regiao['ANO_MES'].dt.to_timestamp()
            casos_por_mes_regiao = casos_por_mes_regiao.drop('ANO_MES', axis=1)
            
            casos_por_mes_regiao['data_clima'] = casos_por_mes_regiao['data_arbovirose'] - pd.DateOffset(months=defasagem_meses)
            casos_por_mes_regiao['mes_arbovirose'] = casos_por_mes_regiao['data_arbovirose'].dt.month
            casos_por_mes_regiao['mes_nome_arbovirose'] = casos_por_mes_regiao['mes_arbovirose'].map(MESES_PT_BR)
            
            df_clima_preparado = df_clima.copy()
            df_clima_preparado = df_clima_preparado.rename(columns={'regiao': 'REGIAO'})
            df_clima_preparado['mes_clima'] = df_clima_preparado['data'].dt.month
            df_clima_preparado['mes_nome_clima'] = df_clima_preparado['mes_clima'].map(MESES_PT_BR)
            
            df_correlacao = pd.merge(
                casos_por_mes_regiao,
                df_clima_preparado,
                left_on=['data_clima', 'REGIAO'],
                right_on=['data', 'REGIAO'],
                how='inner',
                suffixes=('', '_clima')
            )
            
            df_correlacao = df_correlacao.rename(columns={
                'data_arbovirose': 'data_casos',
                'data_clima': 'data_clima_correlacao'
            })
            
            df_correlacao['arbovirose'] = arbovirose
            df_correlacao['ano'] = ano
            df_correlacao['defasagem_meses'] = defasagem_meses
            df_correlacao['relacao_temporal'] = df_correlacao['mes_nome_clima'] + ' → ' + df_correlacao['mes_nome_arbovirose']
            
            logger.info(f"Dados preparados para correlação com defasagem: {len(df_correlacao)} registros")
            logger.info(f"Relação temporal: Clima do mês M → Casos do mês M+{defasagem_meses}")
            
            return df_correlacao
            
        except Exception as e:
            logger.error(f"Erro ao preparar dados para correlação: {e}")
            return pd.DataFrame(columns=['data_casos', 'data_clima_correlacao', 'REGIAO', 'casos_arbovirose', 
                                    'precipitacao_mm', 'temperatura_c', 'umidade_percentual', 'arbovirose', 'ano'])
    
    @staticmethod
    def calcular_correlacao_por_variavel(df_correlacao: pd.DataFrame, variavel_alvo: str) -> Dict:
        """
        Calcula correlação para uma variável climática específica
        
        Args:
            df_correlacao: DataFrame preparado para correlação
            variavel_alvo: Variável climática a analisar
            
        Returns:
            Dicionário com resultados da correlação
        """
        if df_correlacao.empty or variavel_alvo not in df_correlacao.columns:
            return {}
        
        resultados_variavel = {
            'correlacao_geral': {},
            'correlacao_por_regiao': {},
        }
        
        dados_validos = df_correlacao[['casos_arbovirose', variavel_alvo]].dropna()
        if len(dados_validos) > 1:
            corr_spearman, p_spearman = stats.spearmanr(
                dados_validos['casos_arbovirose'], 
                dados_validos[variavel_alvo]
            )
            
            resultados_variavel['correlacao_geral'] = {
                'spearman': round(corr_spearman, 3),
                'p_valor': round(p_spearman, 4),
                'significativo': p_spearman < 0.05
            }
        
        for regiao in df_correlacao['REGIAO'].unique():
            df_regiao = df_correlacao[df_correlacao['REGIAO'] == regiao]
            dados_validos_regiao = df_regiao[['casos_arbovirose', variavel_alvo]].dropna()
            
            if len(dados_validos_regiao) > 1:
                corr_spearman, p_spearman = stats.spearmanr(
                    dados_validos_regiao['casos_arbovirose'], 
                    dados_validos_regiao[variavel_alvo]
                )
                
                resultados_variavel['correlacao_por_regiao'][regiao] = {
                    'spearman': round(corr_spearman, 3),
                    'p_valor': round(p_spearman, 4),
                    'significativo': p_spearman < 0.05,
                    'n_amostras': len(dados_validos_regiao)
                }
        
        return resultados_variavel
    
    @staticmethod
    def gerar_relatorio_por_variavel(resultados_variavel: Dict, variavel_climatica: str,
                                arbovirose: str, ano: int) -> Dict:
        """
        Gera relatório específico para uma variável climática
        
        Args:
            resultados_variavel: Resultados da análise de correlação
            variavel_climatica: Variável climática analisada
            arbovirose: Nome da arbovirose
            ano: Ano dos dados
            
        Returns:
            Dicionário com relatório completo
        """
        relatorio = {
            'resumo_analise': {
                'arbovirose': arbovirose,
                'variavel_climatica': variavel_climatica,
                'ano': ano,
                'data_geracao': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'correlacao_principal': {},
            'insights': [],
            'recomendacoes': []
        }
        
        correlacao_geral = resultados_variavel.get('correlacao_geral', {})
        if correlacao_geral:
            spearman_corr = correlacao_geral['spearman']
            spearman_p = correlacao_geral['p_valor']
            significativo = correlacao_geral['significativo']
            
            intensidade = "forte" if abs(spearman_corr) > 0.6 else "moderada" if abs(spearman_corr) > 0.3 else "fraca"
            direcao = "positiva" if spearman_corr > 0 else "negativa"
            
            relatorio['correlacao_principal'] = {
                'variavel_climatica': variavel_climatica.replace('_', ' ').title(),
                'correlacao_spearman': spearman_corr,
                'p_valor': spearman_p,
                'significativo': significativo,
                'intensidade': intensidade,
                'direcao': direcao
            }
            
            nome_var = variavel_climatica.replace('_', ' ').title()
            if significativo:
                if 'temperatura' in variavel_climatica.lower():
                    if direcao == 'positiva':
                        relatorio['insights'].append(f"Aumento da temperatura está associado a mais casos de {arbovirose}")
                    else:
                        relatorio['insights'].append(f"Diminuição da temperatura está associada a mais casos de {arbovirose}")
                
                elif 'precipitacao' in variavel_climatica.lower():
                    if direcao == 'positiva':
                        relatorio['insights'].append(f"Maior precipitação está associada a mais casos de {arbovirose}")
                    else:
                        relatorio['insights'].append(f"Menor precipitação está associada a mais casos de {arbovirose}")
                
                elif 'umidade' in variavel_climatica.lower():
                    if direcao == 'positiva':
                        relatorio['insights'].append(f"Maior umidade está associada a mais casos de {arbovirose}")
                    else:
                        relatorio['insights'].append(f"Menor umidade está associada a mais casos de {arbovirose}")
            else:
                relatorio['insights'].append(f"Não foi encontrada correlação significativa entre {nome_var} e casos de {arbovirose}")
        
        return relatorio

def analisar_correlacao_por_variavel(df_arboviroses: pd.DataFrame, df_clima: pd.DataFrame,
                                   arbovirose: str, ano: int, variavel_climatica: str, 
                                   usar_cache: bool = True, defasagem_meses: int = 1) -> Tuple[Dict, bool]:
    """
    Analisa correlação com variável climática específica e defasagem temporal
    
    Args:
        df_arboviroses: DataFrame com dados de arboviroses
        df_clima: DataFrame com dados climáticos
        arbovirose: Nome da arbovirose
        ano: Ano dos dados
        variavel_climatica: Variável climática a analisar
        usar_cache: Usar cache para melhor performance
        defasagem_meses: Defasagem temporal em meses
        
    Returns:
        Tuple[Resultados da análise, True se veio do cache]
    """
    cache_key = f"{variavel_climatica}_lag{defasagem_meses}"
    
    if usar_cache:
        dados_cache = CacheManagerCorrelacao.carregar(arbovirose, ano, variavel_climatica=cache_key)
        if dados_cache is not None:
            return dados_cache, True
    
    logger.info(f"Analisando correlação entre {arbovirose} e {variavel_climatica} para {ano} (defasagem: {defasagem_meses} mês(es))...")
    
    df_correlacao = CorrelationAnalyzer.preparar_dados_correlacao(
        df_arboviroses, df_clima, arbovirose, ano, defasagem_meses
    )
    
    if df_correlacao.empty:
        logger.warning("DataFrame de correlação vazio - não é possível calcular correlações")
        resultados_vazio = {
            'dados_correlacao': df_correlacao, 'variavel_analisada': variavel_climatica,
            'defasagem_meses': defasagem_meses,
            'resultados_correlacao': {},
            'relatorio': {
                'resumo_analise': {
                    'arbovirose': arbovirose, 
                    'variavel_climatica': variavel_climatica, 
                    'ano': ano, 
                    'defasagem_meses': defasagem_meses,
                    'data_geracao': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                'correlacao_principal': {}, 
                'insights': ['Dados insuficientes para análise de correlação'], 
                'recomendacoes': []
            }
        }
        return resultados_vazio, False
    
    resultados_variavel = CorrelationAnalyzer.calcular_correlacao_por_variavel(
        df_correlacao, variavel_climatica
    )
    
    relatorio = CorrelationAnalyzer.gerar_relatorio_por_variavel(
        resultados_variavel, variavel_climatica, arbovirose, ano
    )
    
    relatorio['resumo_analise']['defasagem_meses'] = defasagem_meses
    relatorio['correlacao_principal']['defasagem_meses'] = defasagem_meses
    
    resultados_completos = {
        'dados_correlacao': df_correlacao, 
        'variavel_analisada': variavel_climatica,
        'defasagem_meses': defasagem_meses,
        'resultados_correlacao': resultados_variavel, 
        'relatorio': relatorio
    }
    
    cache_utilizado = False
    if usar_cache and resultados_completos:
        cache_salvo = CacheManagerCorrelacao.salvar(arbovirose, ano, resultados_completos, variavel_climatica=cache_key)
        cache_utilizado = cache_salvo
    
    return resultados_completos, cache_utilizado

def limpar_cache_correlacao():
    """Limpa todo o cache de análises de correlação"""
    try:
        if os.path.exists(CORRELACAO_CACHE_DIR):
            for arquivo in os.listdir(CORRELACAO_CACHE_DIR):
                os.remove(os.path.join(CORRELACAO_CACHE_DIR, arquivo))
            logger.info("Cache de análises de correlação limpo com sucesso!")
        else:
            logger.info("Diretório de cache correlação não existe")
    except Exception as e:
        logger.error(f"Erro ao limpar cache correlação: {e}")