"""
Módulo: main.py
Descrição: Dashboard interativo (Streamlit) para visualização e análise de dados de arboviroses e clima
Desenvolvido para: Trabalho de Conclusão de Curso (TCC) - Análise de Dados e Correlação Climática com Arboviroses nas cinco regiões do Brasil
Autor: Eloy Cardozo Augusto
Código: 836463
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.stats import spearmanr

from utils import (
    baixar_dados_arbovirose, 
    agrupar_casos_por_mes, 
    gerar_relatorio_qualidade,
    DataCleaner,
    REGIOES_VALIDAS,
    limpar_cache_arboviroses
)
from utils_climate import (
    CacheManagerClima,
    baixar_dados_climaticos,
    tratar_dados_climaticos,
    gerar_relatorio_clima,
    limpar_cache_clima
)
from correlation_analysis import (
    analisar_correlacao_por_variavel,
    limpar_cache_correlacao
)

if 'recalcular_correlacao' not in st.session_state:
    st.session_state.recalcular_correlacao = False
if 'variavel_climatica_selecionada' not in st.session_state:
    st.session_state.variavel_climatica_selecionada = 'temperatura_c'

st.set_page_config(
    page_title="Análise de Arboviroses e Clima - Brasil", 
    layout="wide",
    page_icon="🦟"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">🦟 Análise de Arboviroses e Clima no Brasil</h1>', unsafe_allow_html=True)

with st.sidebar:
    st.header("⚙️ Configurações")
    
    arboviroses = ["Dengue", "Chikungunya", "Zika"]
    arbovirose = st.selectbox("Escolha a arbovirose:", arboviroses)
    
    anos_disponiveis = {
        "Dengue": (2010, 2025),
        "Chikungunya": (2015, 2025),
        "Zika": (2016, 2025)
    }
    ano_min, ano_max = anos_disponiveis[arbovirose]
    
    ano = st.number_input(
        f"Escolha o ano para análise ({ano_min}-{ano_max}):", 
        min_value=ano_min, 
        max_value=ano_max, 
        value=ano_max, 
        step=1
    )
    
    st.subheader("🔧 Opções de Análise")
    mostrar_relatorio_qualidade = st.checkbox("Mostrar relatório de qualidade", value=True)
    incluir_analise_clima = st.checkbox("Incluir análise climática", value=True)
    analisar_correlacao = st.checkbox("Analisar correlação clima-arboviroses", value=True)
    
    st.subheader("⏰ Defasagem Temporal")
    defasagem = st.slider(
        "Defasagem (meses):",
        min_value=0,
        max_value=3,
        value=1,
        help="Clima do mês M correlacionado com casos do mês M+defasagem"
    )

    st.session_state.defasagem = defasagem
    
    st.subheader("💾 Configurações de Cache")
    usar_cache = st.checkbox("Usar cache para melhor performance", value=True)
    
    col_cache1, col_cache2 = st.columns(2)
    with col_cache1:
        if st.button("🗑️ Limpar Cache", type="secondary", use_container_width=True):
            try:
                limpar_cache_arboviroses()
                limpar_cache_clima()
                limpar_cache_correlacao()
                st.success("✅ Cache limpo com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Erro ao limpar cache: {e}")
    
    with col_cache2:
        if 'forcar_recarregar' not in st.session_state:
            st.session_state.forcar_recarregar = False
            
        if st.button("🔄 Forçar Recarregar", type="secondary", use_container_width=True):
            st.session_state.forcar_recarregar = True
            st.info("🔁 Dados serão recarregados (ignorando cache)")
    
    if incluir_analise_clima:
        st.info("⚠️ A análise climática pode levar alguns minutos")

if 'last_arbovirose' not in st.session_state:
    st.session_state.last_arbovirose = arbovirose
if 'last_ano' not in st.session_state:
    st.session_state.last_ano = ano

if (st.session_state.last_arbovirose != arbovirose) or (st.session_state.last_ano != ano):
    st.session_state.dados_processados = False
    st.session_state.last_arbovirose = arbovirose
    st.session_state.last_ano = ano

col_btn_left, col_btn_center, col_btn_right = st.columns([1, 3, 1])

with col_btn_center:
    btn_processar = st.button("📥 Baixar e Processar Dados", type="primary", use_container_width=True)

dados_ja_processados = (
    btn_processar or 
    st.session_state.get('forcar_recarregar', False) or
    st.session_state.get('dados_processados', False)
)

if dados_ja_processados:
    
    usar_cache_atual = usar_cache and not st.session_state.get('forcar_recarregar', False)
    
    if st.session_state.get('forcar_recarregar', False):
        st.session_state.forcar_recarregar = False
    
    with st.spinner("Baixando e processando dados de arboviroses..."):
        try:
            resultado_arbovirose = baixar_dados_arbovirose(arbovirose, ano, usar_cache=usar_cache_atual)
            
            if isinstance(resultado_arbovirose, tuple):
                df_arboviroses, cache_utilizado_arb = resultado_arbovirose
            else:
                df_arboviroses = resultado_arbovirose
                cache_utilizado_arb = False
            
            if df_arboviroses.empty:
                st.warning(f"⚠️ Nenhum dado encontrado para {arbovirose} no ano {ano}.")
                st.stop()
                
            casos_regiao = agrupar_casos_por_mes(df_arboviroses, ano)
            relatorio_qualidade = gerar_relatorio_qualidade(df_arboviroses)
            
            if cache_utilizado_arb:
                st.success(f"✅ Dados de {arbovirose} - {ano} carregados do cache!")
            else:
                st.success(f"✅ Dados de {arbovirose} - {ano} baixados e processados com sucesso!")
            
            st.session_state.dados_processados = True
            
        except Exception as e:
            st.error(f"❌ Erro ao processar dados de arboviroses: {str(e)}")
            st.session_state.dados_processados = False
            st.stop()

    relatorio_clima = None
    df_clima = pd.DataFrame()
    resultados_correlacao = {}
    correlacoes_significativas = []
    
    if incluir_analise_clima:
        with st.spinner("Baixando e processando dados climáticos..."):
            try:
                cache_existia_antes = CacheManagerClima.existe(ano) if usar_cache_atual else False
                
                resultado_clima = baixar_dados_climaticos(ano, usar_cache=usar_cache_atual)

                if isinstance(resultado_clima, tuple):
                    df_clima_bruto, cache_utilizado_clima = resultado_clima
                else:
                    df_clima_bruto = resultado_clima
                    cache_utilizado_clima = False

                df_clima = tratar_dados_climaticos(df_clima_bruto)
                relatorio_clima = gerar_relatorio_clima(df_clima)

                if cache_utilizado_clima:
                    st.success(f"✅ Dados climáticos - {ano} carregados do cache!")
                else:
                    st.success(f"✅ Dados climáticos - {ano} processados com sucesso!")
                
            except Exception as e:
                st.error(f"❌ Erro ao processar dados climáticos: {str(e)}")
                if analisar_correlacao:
                    st.warning("⚠️ Análise de correlação desativada devido a erro nos dados climáticos")
                    analisar_correlacao = False
    
    if analisar_correlacao and df_clima is not None and not df_clima.empty:
        with st.spinner("Analisando correlação entre clima e arboviroses..."):
            try:
                if 'variavel_climatica_selecionada' in st.session_state:
                    variavel_alvo = st.session_state.variavel_climatica_selecionada
                else:
                    variavel_alvo = 'temperatura_c'
                
                usar_cache_correlacao = usar_cache_atual
                
                if st.session_state.get('recalcular_correlacao', False):
                    usar_cache_correlacao = False
                    st.session_state.recalcular_correlacao = False
                
                resultado_correlacao = analisar_correlacao_por_variavel(
                    df_arboviroses, 
                    df_clima, 
                    arbovirose, 
                    ano, 
                    variavel_climatica=variavel_alvo,
                    usar_cache=usar_cache_correlacao,
                    defasagem_meses=st.session_state.defasagem
                )
                
                if isinstance(resultado_correlacao, tuple):
                    resultados_correlacao, cache_utilizado_corr = resultado_correlacao
                else:
                    resultados_correlacao = resultado_correlacao
                    cache_utilizado_corr = False
                
                if cache_utilizado_corr:
                    st.success(f"✅ Análise de correlação com {variavel_alvo.replace('_', ' ')} carregada do cache!")
                else:
                    st.success(f"✅ Análise de correlação com {variavel_alvo.replace('_', ' ')} concluída com sucesso!")
                
                st.session_state.ultima_variavel_analisada = variavel_alvo
                    
            except Exception as e:
                st.error(f"❌ Erro na análise de correlação: {str(e)}")
    
    if mostrar_relatorio_qualidade:
        st.header("📋 Relatórios de Qualidade dos Dados")
        
        tab_qualidade_arb, tab_qualidade_clima = st.tabs(["🦟 Dados de Arboviroses", "🌤️ Dados Climáticos"])
        
        with tab_qualidade_arb:
            if relatorio_qualidade:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        label="Total de Registros", 
                        value=f"{relatorio_qualidade['total_registros']:,}"
                    )
                
                with col2:
                    casos_investigacao = relatorio_qualidade.get('casos_investigacao', 0)
                    st.metric(
                        label="Casos em Investigação", 
                        value=f"{casos_investigacao:,}"
                    )
                
                with col3:
                    completude = relatorio_qualidade['completude_campos'].get('DT_NOTIFIC', {})
                    if completude:
                        st.metric(
                            label="Completude das Datas", 
                            value=f"{completude['percentual']:.1f}%"
                        )
                
                with col4:
                    distribuicao_regiao = relatorio_qualidade.get('distribuicao_regiao', {})
                    regioes_com_dados = [regiao for regiao in distribuicao_regiao.keys() if regiao in REGIOES_VALIDAS]
                    st.metric(
                        label="Regiões com Dados", 
                        value=len(regioes_com_dados)
                    )
        
        with tab_qualidade_clima:
            if relatorio_clima:
                st.subheader("🔍 Diagnóstico Detalhado dos Dados Climáticos")
                
                col_diagn1, col_diagn2 = st.columns([2, 1])
                
                with col_diagn1:
                    for regiao in df_clima['regiao'].unique():
                        df_regiao = df_clima[df_clima['regiao'] == regiao]
                        
                        with st.expander(f"📊 {regiao} - Qualidade dos Dados", expanded=True):
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                if 'temperatura_c' in df_regiao.columns:
                                    temp_data = df_regiao['temperatura_c']
                                    st.metric("🌡️ Temperatura", f"{temp_data.mean():.1f}°C")
                                    st.caption(f"Variação: {temp_data.min():.1f} a {temp_data.max():.1f}°C")
                                    st.caption(f"Meses: {len(df_regiao)}")
                            
                            with col2:
                                if 'precipitacao_mm' in df_regiao.columns:
                                    precip_data = df_regiao['precipitacao_mm']
                                    st.metric("🌧️ Precipitação", f"{precip_data.mean():.1f}mm")
                                    st.caption(f"Total: {precip_data.sum():.0f}mm")
                                    st.caption(f"Zeros: {(precip_data == 0).sum()} meses")
                            
                            with col3:
                                if 'umidade_percentual' in df_regiao.columns:
                                    umid_data = df_regiao['umidade_percentual']
                                    st.metric("💧 Umidade", f"{umid_data.mean():.1f}%")
                                    st.caption(f"Variação: {umid_data.min():.1f} a {umid_data.max():.1f}%")
                                    st.caption(f"Missing: {umid_data.isnull().sum()}")
                
                with col_diagn2:
                    st.subheader("📈 Estatísticas Gerais")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric(
                            label="Total de Registros Climáticos", 
                            value=f"{relatorio_clima['total_registros']:,}"
                        )
                    
                    with col2:
                        regioes_clima = len(relatorio_clima['regioes'])
                        st.metric(
                            label="Regiões com Dados Climáticos", 
                            value=regioes_clima
                        )
                    
                    dados_faltantes = relatorio_clima.get('dados_faltantes', {})
                    total_campos = len(dados_faltantes)
                    completos = sum(1 for var in dados_faltantes.values() if var['percentual'] == 0)
                    if total_campos > 0:
                        percentual_completo = (completos / total_campos) * 100
                        st.metric(
                            label="Completude dos Dados Climáticos", 
                            value=f"{percentual_completo:.1f}%"
                        )
                
                st.subheader("📋 Dados Detalhados por Mês e Região")
                st.dataframe(df_clima, use_container_width=True)
                
                with st.expander("ℹ️ Como interpretar este diagnóstico"):
                    st.markdown("""
                    **📊 O que observar:**
                    - 🌡️ **Temperatura**: Deve variar entre 15-35°C (dependendo da região)
                    - 🌧️ **Precipitação**: Zeros são normais em meses secos
                    - 💧 **Umidade**: Deve estar entre 40-95%
                    - 📅 **Meses**: Idealmente 12 meses completos por região
                    
                    **⚠️ Problemas comuns:**
                    - Temperatura constante (interpolação excessiva)
                    - Precipitação sempre zero 
                    - Umidade fora da faixa realista
                    - Menos de 12 meses de dados
                    """)
    
    st.header("🔍 Estatísticas Gerais")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_casos = len(df_arboviroses)
        st.metric("Total de Casos", f"{total_casos:,}")
    
    with col2:
        if not df_arboviroses.empty and 'REGIAO' in df_arboviroses.columns:
            df_regioes_validas = df_arboviroses[df_arboviroses['REGIAO'].isin(REGIOES_VALIDAS)]
            if not df_regioes_validas.empty:
                dist_regiao = df_regioes_validas['REGIAO'].value_counts(normalize=True) * 100
                regiao_mais_afetada = dist_regiao.index[0]
                percentual_mais_afetada = dist_regiao.iloc[0]
                
                st.metric("Região Mais Afetada", f"{regiao_mais_afetada}")
                st.caption(f"({percentual_mais_afetada:.1f}% dos casos)")
            else:
                st.metric("Região Mais Afetada", "N/A")
        else:
            st.metric("Região Mais Afetada", "N/A")
    
    with col3:
        if not df_arboviroses.empty and 'CS_SEXO' in df_arboviroses.columns:
            distribuicao_sexo = df_arboviroses['CS_SEXO'].value_counts()
            sexo_mais_comum = distribuicao_sexo.index[0] if not distribuicao_sexo.empty else "N/A"
            st.metric("Sexo Mais Comum", sexo_mais_comum)
        else:
            st.metric("Sexo Mais Comum", "N/A")
    
    with col4:
        if not casos_regiao.empty:
            casos_regiao_validos = casos_regiao[casos_regiao['REGIAO'].isin(REGIOES_VALIDAS)]
            if not casos_regiao_validos.empty:
                mes_pico = casos_regiao_validos.loc[casos_regiao_validos['CASOS'].idxmax(), 'MES_FORMATADO']
                st.metric("Mês de Pico", mes_pico)
            else:
                st.metric("Mês de Pico", "N/A")
        else:
            st.metric("Mês de Pico", "N/A")
    
    if incluir_analise_clima and df_clima is not None:
        st.header("🌤️ Análise Climática por Região")
        
        variaveis_climaticas_relevantes = ['precipitacao_mm', 'temperatura_c', 'umidade_percentual']
        variaveis_existentes = [var for var in variaveis_climaticas_relevantes if var in df_clima.columns]
        
        if variaveis_existentes:
            if 'variavel_climatica_selecionada' not in st.session_state:
                st.session_state.variavel_climatica_selecionada = 'temperatura_c'
            
            variavel_selecionada = st.selectbox(
                "Selecione a variável climática:",
                variaveis_existentes,
                index=variaveis_existentes.index(st.session_state.variavel_climatica_selecionada) 
                if st.session_state.variavel_climatica_selecionada in variaveis_existentes 
                else 0,
                key='variavel_climatica_select'
            )

            if variavel_selecionada != st.session_state.variavel_climatica_selecionada:
                from correlation_analysis import CacheManagerCorrelacao
                variavel_anterior = st.session_state.variavel_climatica_selecionada
                CacheManagerCorrelacao.limpar_variavel_especifica(arbovirose, ano, variavel_anterior)
                
                st.session_state.variavel_climatica_selecionada = variavel_selecionada
                st.session_state.recalcular_correlacao = True
                st.rerun()
            
            MESES_PT_BR = {
                1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
                7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
            }

            df_clima_display = df_clima.copy()
            df_clima_display['mes_nome'] = df_clima_display['data'].dt.month.map(MESES_PT_BR)
            df_clima_display['mes_ordem'] = df_clima_display['data'].dt.month

            df_clima_display = df_clima_display.sort_values('mes_ordem')

            fig_clima = px.line(
                df_clima_display, 
                x='mes_nome', 
                y=variavel_selecionada,
                color='regiao',
                title=f'Evolução de {variavel_selecionada.replace("_", " ").title()} por Região - {ano}',
                markers=True
            )
            fig_clima.update_layout(
                xaxis_title="Mês",
                yaxis_title=variavel_selecionada.replace("_", " ").title(),
                template="plotly_white",
                height=400
            )
            st.plotly_chart(fig_clima, use_container_width=True)
            
            st.subheader("📊 Estatísticas Climáticas por Região")
            stats_clima = df_clima.groupby('regiao')[variavel_selecionada].agg(['mean', 'min', 'max', 'std']).round(2)
            st.dataframe(stats_clima, use_container_width=True)
    
    if 'resultados_correlacao' in locals() and resultados_correlacao:
        st.header("🔗 Análise de Correlação: Clima vs Arboviroses")

        relatorio_corr = resultados_correlacao.get('relatorio', {})
        dados_correlacao = resultados_correlacao.get('dados_correlacao')
        
        corr_principal = relatorio_corr.get('correlacao_principal', {})

        st.subheader(f"📈 Resultado para: {st.session_state.get('variavel_climatica_selecionada', 'N/A').replace('_', ' ').title()}")

        if corr_principal:
            with st.container(border=True):
                col1, col2, col3 = st.columns([1.5, 1, 1])

                with col1:
                    intensidade = corr_principal.get('intensidade', 'N/A')
                    direcao = corr_principal.get('direcao', 'N/A')
                    st.markdown(f"**Interpretação:** Correlação **{direcao}** e **{intensidade}**.")

                with col2:
                    corr_val = corr_principal.get('correlacao_spearman', 0.0)
                    st.metric(
                        label="Correlação Spearman (ρ)",
                        value=f"{corr_val:.3f}",
                        help="Mede a força e a direção da associação entre duas variáveis. Varia de -1 (correlação negativa perfeita) a +1 (correlação positiva perfeita)."
                    )

                with col3:
                    p_valor = corr_principal.get('p_valor', 1.0)
                    significativo = p_valor < 0.05
                    
                    st.metric(
                        label="Valor-p",
                        value=f"{p_valor:.4f}",
                        delta="Significativo" if significativo else "Não Significativo",
                        delta_color="inverse" if significativo else "off",
                        help="Probabilidade de obter os resultados observados se não houvesse correlação real. Um valor-p < 0.05 é tipicamente considerado estatisticamente significativo."
                    )

            st.subheader("💡 Insights da Análise")
            insights = relatorio_corr.get('insights', [])
            if insights:
                for insight in insights:
                    st.info(insight)
            else:
                st.info("Nenhum insight automatizado gerado para esta correlação.")
            
            st.subheader("📊 Mapas de Calor de Correlações por Região")

            if dados_correlacao is not None and not dados_correlacao.empty:
                regioes_disponiveis = dados_correlacao['REGIAO'].unique().tolist()
                regioes_disponiveis.insert(0, 'Todas as Regiões')
                
                regiao_selecionada_heatmap = st.selectbox(
                    "Selecione a região para visualizar o mapa de calor:",
                    regioes_disponiveis,
                    key=f'heatmap_region_selector_{arbovirose}_{ano}'
                )
                
                variaveis_correlacao = ['casos_arbovirose', 'precipitacao_mm', 'temperatura_c', 'umidade_percentual']
                variaveis_existentes = [var for var in variaveis_correlacao if var in dados_correlacao.columns]
                
                if len(variaveis_existentes) > 1:
                    
                    if regiao_selecionada_heatmap == 'Todas as Regiões':
                        dados_heatmap = dados_correlacao
                        titulo_heatmap = f'Todas as Regiões - {arbovirose} {ano}'
                    else:
                        dados_heatmap = dados_correlacao[dados_correlacao['REGIAO'] == regiao_selecionada_heatmap]
                        titulo_heatmap = f'{regiao_selecionada_heatmap} - {arbovirose} {ano}'
                    
                    if not dados_heatmap.empty:
                        corr_matrix = dados_heatmap[variaveis_existentes].corr(method='spearman')
                        
                        fig_heatmap = px.imshow(
                            corr_matrix,
                            x=corr_matrix.columns,
                            y=corr_matrix.index,
                            color_continuous_scale='RdBu_r',
                            aspect="auto",
                            title=f'Mapa de Calor: {titulo_heatmap} (Defasagem: {st.session_state.defasagem} mês(es))',
                            labels=dict(x="Variáveis", y="Variáveis", color="Correlação"),
                            zmin=-1,
                            zmax=1
                        )
                        
                        for i in range(len(corr_matrix)):
                            for j in range(len(corr_matrix)):
                                fig_heatmap.add_annotation(
                                    x=j, y=i,
                                    text=f"{corr_matrix.iloc[i, j]:.2f}",
                                    showarrow=False,
                                    font=dict(
                                        color="white" if abs(corr_matrix.iloc[i, j]) > 0.5 else "black",
                                        size=12
                                    )
                                )
                        
                        fig_heatmap.update_layout(
                            height=500,
                            xaxis_title="Variáveis Climáticas",
                            yaxis_title="Variáveis de Casos",
                            font=dict(size=12)
                        )
                        
                        labels_melhorados = {
                            'casos_arbovirose': f'Casos {arbovirose}',
                            'precipitacao_mm': 'Precipitação (mm)',
                            'temperatura_c': 'Temperatura (°C)',
                            'umidade_percentual': 'Umidade (%)'
                        }
                        
                        fig_heatmap.update_xaxes(
                            ticktext=[labels_melhorados.get(col, col) for col in corr_matrix.columns],
                            tickvals=list(range(len(corr_matrix.columns)))
                        )
                        
                        fig_heatmap.update_yaxes(
                            ticktext=[labels_melhorados.get(col, col) for col in corr_matrix.index],
                            tickvals=list(range(len(corr_matrix.index)))
                        )
                        
                        st.plotly_chart(fig_heatmap, use_container_width=True)
                        
                        st.subheader("📈 Estatísticas da Análise")

                        if regiao_selecionada_heatmap == 'Todas as Regiões':
                            dados_estatisticas = dados_correlacao
                            regiao_texto = "Todas as Regiões"
                            total_regioes = len(dados_correlacao['REGIAO'].unique())
                            total_texto = f"{total_regioes}"
                        else:
                            dados_estatisticas = dados_correlacao[dados_correlacao['REGIAO'] == regiao_selecionada_heatmap]
                            regiao_texto = regiao_selecionada_heatmap
                            total_texto = f"{len(dados_estatisticas)} registros"

                        with st.container(key=f"stats_container_{regiao_selecionada_heatmap}", border=True):
                            st.markdown(f"**Região comprovada:** {regiao_texto}")
                            st.markdown(f"**Total de:** {total_texto}")

                            if not dados_estatisticas.empty:
                                temp_media = dados_estatisticas['temperatura_c'].mean() if 'temperatura_c' in dados_estatisticas else None
                                precip_media = dados_estatisticas['precipitacao_mm'].mean() if 'precipitacao_mm' in dados_estatisticas else None
                                umidade_media = dados_estatisticas['umidade_percentual'].mean() if 'umidade_percentual' in dados_estatisticas else None

                                if temp_media is not None:
                                    st.markdown(f"**Temperatura média:** {temp_media:.1f}°C")
                                if precip_media is not None:
                                    st.markdown(f"**Precipitação média:** {precip_media:.1f}mm")
                                if umidade_media is not None:
                                    st.markdown(f"**Umidade média:** {umidade_media:.1f}%")

                                st.markdown("**Correlação principal comprovada:**")
                                variavel_alvo = st.session_state.get('variavel_climatica_selecionada', 'temperatura_c')
                                
                                if variavel_alvo in dados_estatisticas.columns and 'casos_arbovirose' in dados_estatisticas.columns and len(dados_estatisticas) >= 2:
                                    correlacao_principal, p_valor_regional = spearmanr(
                                        dados_estatisticas['casos_arbovirose'], 
                                        dados_estatisticas[variavel_alvo]
                                    )
                                    
                                    significativo = p_valor_regional < 0.05
                                    intensidade = "forte" if abs(correlacao_principal) > 0.6 else "moderada" if abs(correlacao_principal) > 0.3 else "fraca"
                                    direcao = "positiva" if correlacao_principal > 0 else "negativa"
                                    
                                    p_valor_status = "Significativo" if significativo else "Não Significativo"

                                    st.markdown(
                                        f"- **{variavel_alvo.replace('_', ' ').title()}** : **{correlacao_principal:.3f}** "
                                        f"({direcao} e {intensidade}) "
                                        f"| **P-valor**: **{p_valor_regional:.4f}** ({p_valor_status})"
                                    )
                                else:
                                    st.warning("Não há dados suficientes para calcular a correlação.")
                            else:
                                st.warning("Não há dados suficientes para calcular estatísticas.")

                    with st.expander("ℹ️ Como interpretar os mapas de calor:"):
                        st.markdown("""
                        **🎯 Interpretação das Cores:**
                        - **🔴 Vermelho Escuro**: Correlação positiva forte (próximo de +1)
                        - **🔴 Vermelho Claro**: Correlação positiva fraca
                        - **⚪ Branco**: Sem correlação (próximo de 0)  
                        - **🔵 Azul Claro**: Correlação negativa fraca
                        - **🔵 Azul Escuro**: Correlação negativa forte (próximo de -1)
                        
                        **📊 Escala de Intensidade:**
                        - **0.7 a 1.0**: Correlação muito forte
                        - **0.5 a 0.7**: Correlação forte
                        - **0.3 a 0.5**: Correlação moderada
                        - **0.1 a 0.3**: Correlação fraca
                        - **0.0 a 0.1**: Sem correlação prática
                        
                        **💡 Dica:** Compare diferentes regiões para identificar padrões regionais!
                        """)
                else:
                    st.warning("Não há variáveis suficientes para gerar o mapa de calor")
            else:
                st.warning("Não há dados de correlação disponíveis para gerar os mapas de calor")

        else:
            st.warning("ℹ️ Os resultados da análise de correlação não puderam ser processados para exibição. Verifique os logs para mais detalhes.")

    st.header("📈 Visualizações Principais")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🗺️ Distribuição Regional", 
        "📊 Por Região", 
        "🔥 Mapa de Calor", 
        "📋 Tabelas",
        "📈 Dispersão Clima-Casos"
    ])
    
    with tab1:
        st.subheader("🗺️ Distribuição de Casos por Região")
        
        if not df_arboviroses.empty and 'REGIAO' in df_arboviroses.columns:
            df_regioes_validas = df_arboviroses[df_arboviroses['REGIAO'].isin(REGIOES_VALIDAS)]
            
            if not df_regioes_validas.empty:
                dist_regiao = df_regioes_validas['REGIAO'].value_counts()
                total_casos = dist_regiao.sum()
                
                df_regioes = pd.DataFrame({
                    'Região': dist_regiao.index,
                    'Casos': dist_regiao.values,
                    'Percentual': (dist_regiao.values / total_casos * 100).round(1)
                })
                
                fig_pizza = px.pie(
                    df_regioes, 
                    values='Casos', 
                    names='Região',
                    title=f'Distribuição de Casos de {arbovirose} por Região - {ano}',
                    hover_data=['Percentual'],
                    labels={'Percentual': '%'}
                )
                
                fig_pizza.update_traces(
                    textposition='inside',
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b><br>Casos: %{value:,}<br>Percentual: %{percent}'
                )
                
                fig_pizza.update_layout(height=500, showlegend=True)
                st.plotly_chart(fig_pizza, use_container_width=True)
                
                st.subheader("📊 Dados Detalhados por Região")
                st.dataframe(df_regioes, use_container_width=True)
            else:
                st.warning("Não há dados de regiões válidas disponíveis")
        else:
            st.warning("Não há dados de região disponíveis")
    
    with tab2:
        st.subheader("📈 Evolução Mensal por Região")
        
        if not casos_regiao.empty:
            casos_regiao_validos = casos_regiao[casos_regiao['REGIAO'].isin(REGIOES_VALIDAS)]
            regioes = casos_regiao_validos['REGIAO'].unique()
            
            if len(regioes) > 0:
                col1, col2 = st.columns(2)
                
                for i, regiao in enumerate(regioes):
                    with col1 if i % 2 == 0 else col2:
                        dados_regiao = casos_regiao_validos[casos_regiao_validos["REGIAO"] == regiao]
                        if not dados_regiao.empty:
                            fig = px.line(
                                dados_regiao, 
                                x="MES_FORMATADO", 
                                y="CASOS",
                                title=f"{regiao} - {ano}",
                                markers=True,
                                color_discrete_sequence=[px.colors.qualitative.Set1[i % 10]]
                            )
                            fig.update_layout(
                                xaxis_title="Mês", 
                                yaxis_title="Número de Casos", 
                                template="plotly_white",
                                height=300
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning(f"Sem dados para {regiao}")
            else:
                st.warning("Não há dados de regiões válidas para gerar os gráficos")
        else:
            st.warning("Não há dados suficientes para gerar os gráficos por região")
    
    with tab3:
        st.subheader("🔥 Mapa de Calor - Casos por Região e Mês")
        if not casos_regiao.empty:
            casos_regiao_validos = casos_regiao[casos_regiao['REGIAO'].isin(REGIOES_VALIDAS)]
            
            if not casos_regiao_validos.empty:
                pivot_table = casos_regiao_validos.pivot_table(
                    index='REGIAO', 
                    columns='MES_FORMATADO', 
                    values='CASOS', 
                    aggfunc='sum', 
                    fill_value=0
                )
                
                fig = go.Figure(data=go.Heatmap(
                    z=pivot_table.values,
                    x=pivot_table.columns,
                    y=pivot_table.index,
                    colorscale='Reds',
                    hoverongaps=False,
                    hovertemplate='Região: %{y}<br>Mês: %{x}<br>Casos: %{z}<extra></extra>',
                    colorbar=dict(title='Número de Casos')
                ))
                
                fig.update_layout(
                    title=f'Mapa de Calor de Casos de {arbovirose} por Região e Mês - {ano}',
                    xaxis_title='Mês',
                    yaxis_title='Região',
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Não há dados de regiões válidas para gerar o mapa de calor")
        else:
            st.warning("Não há dados suficientes para gerar o mapa de calor")
    
    with tab4:
        st.subheader("📋 Dados Detalhados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Casos por Região e Mês**")
            if not casos_regiao.empty:
                casos_regiao_validos = casos_regiao[casos_regiao['REGIAO'].isin(REGIOES_VALIDAS)]
                st.dataframe(casos_regiao_validos, use_container_width=True)
            else:
                st.warning("Não há dados para exibir")
        
        with col2:
            st.write("**Distribuição por Região**")
            if not df_arboviroses.empty and 'REGIAO' in df_arboviroses.columns:
                df_regioes_validas = df_arboviroses[df_arboviroses['REGIAO'].isin(REGIOES_VALIDAS)]
                if not df_regioes_validas.empty:
                    distribuicao_regiao = df_regioes_validas['REGIAO'].value_counts().reset_index()
                    distribuicao_regiao.columns = ['REGIAO', 'CASOS']
                    distribuicao_regiao['%'] = (distribuicao_regiao['CASOS'] / distribuicao_regiao['CASOS'].sum() * 100).round(2)
                    st.dataframe(distribuicao_regiao, use_container_width=True)
                else:
                    st.warning("Não há dados de regiões válidas disponíveis")
            else:
                st.warning("Não há dados de região disponíveis")
        
    with tab5:
        st.subheader("📈 Gráfico de Dispersão: Clima vs Casos")
        
        if 'resultados_correlacao' in locals() and resultados_correlacao and not resultados_correlacao.get('dados_correlacao', pd.DataFrame()).empty:
            
            dados_dispersao = resultados_correlacao['dados_correlacao']
            variavel_atual = st.session_state.get('variavel_climatica_selecionada', 'temperatura_c')
            
            col_controls1, col_controls2 = st.columns(2)
            
            with col_controls1:
                variaveis_disponiveis = [var for var in ['temperatura_c', 'precipitacao_mm', 'umidade_percentual'] 
                                    if var in dados_dispersao.columns]
                variavel_dispersao = st.selectbox(
                    "Variável climática:",
                    variaveis_disponiveis,
                    index=variaveis_disponiveis.index(variavel_atual) if variavel_atual in variaveis_disponiveis else 0,
                    key='dispersao_var_select'
                )
            
            with col_controls2:
                regioes_dispersao = ['Todas as Regiões'] + dados_dispersao['REGIAO'].unique().tolist()
                regiao_dispersao = st.selectbox(
                    "Filtrar por região:",
                    regioes_dispersao,
                    key='dispersao_regiao_select'
                )
            
            if regiao_dispersao == 'Todas as Regiões':
                dados_filtrados = dados_dispersao
                titulo_regiao = "Todas as Regiões"
            else:
                dados_filtrados = dados_dispersao[dados_dispersao['REGIAO'] == regiao_dispersao]
                titulo_regiao = regiao_dispersao
            
            if not dados_filtrados.empty:
                fig_dispersao = px.scatter(
                    dados_filtrados,
                    x=variavel_dispersao,
                    y='casos_arbovirose',
                    color='REGIAO' if regiao_dispersao == 'Todas as Regiões' else None,
                    title=f'Relação entre {variavel_dispersao.replace("_", " ").title()} e Casos de {arbovirose} - {titulo_regiao}',
                    labels={
                        variavel_dispersao: variavel_dispersao.replace('_', ' ').title(),
                        'casos_arbovirose': f'Casos de {arbovirose}',
                        'REGIAO': 'Região'
                    },
                    hover_data=['mes_nome_clima', 'mes_nome_arbovirose', 'relacao_temporal'],
                    trendline='lowess'
                )
                
                fig_dispersao.update_layout(
                    height=500,
                    template="plotly_white",
                    xaxis_title=variavel_dispersao.replace('_', ' ').title(),
                    yaxis_title=f'Casos de {arbovirose}',
                    showlegend=(regiao_dispersao == 'Todas as Regiões')
                )
                
                fig_dispersao.update_traces(
                    hovertemplate=(
                        f"<b>{variavel_dispersao.replace('_', ' ').title()}:</b> %{{x}}<br>"
                        f"<b>Casos {arbovirose}:</b> %{{y}}<br>"
                        "<b>Relação temporal:</b> %{customdata[2]}<br>"
                        "<extra></extra>"
                    )
                )
                
                st.plotly_chart(fig_dispersao, use_container_width=True)
                
                st.subheader("📊 Estatísticas da Relação")
                
                if not dados_filtrados.empty:
                    col_stat1, col_stat2, col_stat3 = st.columns(3)
                    
                    with col_stat1:
                        correlacao = dados_filtrados[['casos_arbovirose', variavel_dispersao]].corr(method='spearman').iloc[0,1]
                        st.metric(
                            "Correlação Spearman",
                            f"{correlacao:.3f}",
                            help="Correlação de postos de Spearman entre as variáveis"
                        )
                    
                    with col_stat2:
                        total_pontos = len(dados_filtrados)
                        st.metric("Total de Observações", f"{total_pontos}")
                    
                    with col_stat3:
                        from scipy import stats
                        corr_spearman, p_value = stats.spearmanr(
                            dados_filtrados[variavel_dispersao], 
                            dados_filtrados['casos_arbovirose']
                        )
                        significativo = p_value < 0.05
                        st.metric(
                            "Significância Estatística", 
                            "Significativo" if significativo else "Não Significativo",
                            delta=f"p-value: {p_value:.4f}",
                            delta_color="normal" if significativo else "off"
                        )
                
                with st.expander("ℹ️ Como interpretar este gráfico"):
                    st.markdown("""
                    **📈 Interpretação do Gráfico de Dispersão:**
                    
                    - **Linha azul**: Tendência suavizada (LOWESS) mostrando o padrão geral
                    - **Cores diferentes**: Cada região (quando "Todas as Regiões" selecionado)
                    - **Eixo X**: Variável climática selecionada
                    - **Eixo Y**: Número de casos da arbovirose
                    
                    **🔍 Padrões a observar:**
                    - **Relação positiva**: Pontos seguem tendência ↗️ (mais clima = mais casos)
                    - **Relação negativa**: Pontos seguem tendência ↘️ (mais clima = menos casos)  
                    - **Sem relação**: Pontos dispersos sem padrão claro
                    - **Agrupamentos**: Revelam comportamentos sazonais ou regionais
                    """)
            
            else:
                st.warning("Não há dados disponíveis para a região selecionada")
        
        else:
            st.warning(
                "Os gráficos de dispersão requerem análise de correlação. "
                "Ative 'Analisar correlação clima-arboviroses' nas configurações."
            )
    
    st.header("📋 Relatório Analítico Final")
    
    if not df_arboviroses.empty:
        if 'REGIAO' in df_arboviroses.columns:
            df_regioes_validas = df_arboviroses[df_arboviroses['REGIAO'].isin(REGIOES_VALIDAS)]
            if not df_regioes_validas.empty:
                percentual_regiao = (df_regioes_validas['REGIAO'].value_counts(normalize=True) * 100).round(2)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total de Casos", f"{len(df_arboviroses):,}")
        
        with col2:
            if 'REGIAO' in df_arboviroses.columns and 'percentual_regiao' in locals() and not percentual_regiao.empty:
                st.metric("Região Mais Afetada", 
                         f"{percentual_regiao.index[0]} ({percentual_regiao.iloc[0]}%)")
            else:
                st.metric("Região Mais Afetada", "N/A")
        
        with col3:
            if not casos_regiao.empty:
                casos_regiao_validos = casos_regiao[casos_regiao['REGIAO'].isin(REGIOES_VALIDAS)]
                if not casos_regiao_validos.empty:
                    mes_pico = casos_regiao_validos.loc[casos_regiao_validos['CASOS'].idxmax(), 'MES_FORMATADO']
                    st.metric("Mês de Pico Nacional", mes_pico)
                else:
                    st.metric("Mês de Pico Nacional", "N/A")
            else:
                st.metric("Mês de Pico Nacional", "N/A")
        
        if 'resultados_correlacao' in locals() and resultados_correlacao:
            relatorio_corr = resultados_correlacao.get('relatorio', {})
            corr_principal = relatorio_corr.get('correlacao_principal', {})
            if corr_principal and corr_principal.get('significativo'):
                st.subheader("🔗 Insights de Correlação Clima-Arboviroses")
                var_nome = corr_principal['variavel_climatica'].replace('_', ' ').title()
                st.info(
                    f"**{var_nome}**: Encontrada uma correlação **{corr_principal['direcao']}** e **{corr_principal['intensidade']}** "
                    f"(ρ = {corr_principal['correlacao_spearman']:.3f}) com os casos de {arbovirose}."
                )

        st.subheader("💡 Insights Epidemiológicos")
        
        insights = []
        
        if 'REGIAO' in df_arboviroses.columns and 'percentual_regiao' in locals() and not percentual_regiao.empty and percentual_regiao.iloc[0] > 40:
            insights.append(f"• **Concentração regional**: {percentual_regiao.index[0]} concentra {percentual_regiao.iloc[0]}% dos casos")
        
        if not casos_regiao.empty and len(casos_regiao) > 1:
            casos_regiao_validos = casos_regiao[casos_regiao['REGIAO'].isin(REGIOES_VALIDAS)]
            if not casos_regiao_validos.empty:
                casos_por_mes = casos_regiao_validos.groupby('MES_FORMATADO')['CASOS'].sum()
                if len(casos_por_mes) > 1 and casos_por_mes.iloc[0] > 0:
                    variacao = ((casos_por_mes.iloc[-1] - casos_por_mes.iloc[0]) / casos_por_mes.iloc[0]) * 100
                    
                    if variacao > 50:
                        insights.append(f"• **Crescimento significativo**: Aumento de {variacao:+.1f}% em relação ao início do ano")
                    elif variacao < -50:
                        insights.append(f"• **Redução significativa**: Queda de {abs(variacao):.1f}% em relação ao início do ano")
        
        if not insights:
            insights.append("• **Padrão estável**: Distribuição relativamente equilibrada entre regiões e ao longo do ano")
        
        for insight in insights:
            st.write(insight)

else:
    st.info("👆 Selecione uma arbovirose e ano acima, depois clique em 'Baixar e Processar Dados' para iniciar a análise.")

st.markdown("---")
st.markdown(
    "**Desenvolvido para Análise e correlação de dados Climáticos com Arboviroses** | "
    "Fontes: DATASUS/SINAN • BDMEP/INMET | "
    "Dados sujeitos a revisão"
)