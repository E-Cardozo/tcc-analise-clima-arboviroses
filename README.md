# tcc-analise-clima-arboviroses
Ferramenta computacional para o TCC de Eng. Computação, correlacionando dados epidemiológicos (DATASUS/SINAN) e climáticos (BDMEP/INMET).

# 🦟 Análise de Correlação: Clima e Arboviroses no Brasil (TCC)

**Projeto de Trabalho de Conclusão de Curso (TCC)**
**Autor:** Eloy Cardozo Augusto
**Código:** 836463

## 📝 Descrição do Projeto

Este projeto consiste em um dashboard interativo desenvolvido em **Streamlit** como parte do Trabalho de Conclusão de Curso. A ferramenta computacional realiza a análise de dados e a correlação entre variáveis climáticas (obtidas do INMET) e a incidência de casos de arboviroses (Dengue, Zika e Chikungunya, obtidos do SINAN/DATASUS) nas cinco macrorregiões do Brasil.

O dashboard permite ao usuário selecionar a arbovirose, o ano de análise e a defasagem temporal (em meses) para estudar como fatores como temperatura, precipitação e umidade podem influenciar a disseminação dessas doenças.

## 🚀 Funcionalidades Principais

* **Dashboard Interativo:** Interface amigável com Streamlit para seleção de filtros.
* **Extração de Dados (Arboviroses):** Download e processamento de dados epidemiológicos (SINAN) via `utils.py`.
* **Extração de Dados (Clima):** Download e processamento de dados climáticos (INMET) via `utils_climate.py`.
* **Sistema de Cache:** Utiliza cache local (`.pkl`) para acelerar carregamentos subsequentes.
* **Análise de Correlação:** Calcula a correlação de Spearman (com Valor-p) entre os casos e as variáveis climáticas, permitindo defasagem temporal (`correlation_analysis.py`).
* **Visualização de Dados:**
    * Gráficos de série temporal (casos e clima).
    * Mapas de calor (Heatmaps) de correlação por região.
    * Gráficos de dispersão (Scatter plots) para análise visual da correlação.
    * Relatórios de qualidade dos dados brutos.

## 🛠️ Tecnologias Utilizadas

O projeto foi construído utilizando as seguintes tecnologias (vide `requirements.txt`):

* **Python 3**
* **Streamlit:** Para a criação do dashboard web interativo.
* **Pandas:** Para manipulação e análise de dados.
* **Plotly:** Para a geração de gráficos interativos.
* **Requests:** Para o download dos dados das fontes oficiais.
* **Scipy:** Para os cálculos estatísticos (Correlação de Spearman).

## 📂 Estrutura do Projeto

A estrutura de arquivos foi organizada para modularizar as responsabilidades:
TCC/
├── dados/
│   ├── arboviroses/          (Cache de dados .pkl)
│   ├── clima/                (Cache de dados .pkl)
│   └── correlacao/           (Cache de dados .pkl)
├── venv/
├── .gitignore
├── correlation_analysis.py   (Módulo de análise correlacional)
├── main.py                   (Arquivo principal da aplicação Streamlit)
├── requirements.txt          (Dependências do projeto)
├── utils_climate.py          (Utilitários para processamento de dados climáticos)
└── utils.py                  (Utilitários para processamento de dados epidemiológicos)

## 📊 Fontes dos Dados

* **Dados Epidemiológicos (Arboviroses):** [SINAN - DATASUS](https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/)
* **Dados Climáticos (Meteorologia):** [BDMEP - INMET - Portal de Dados Históricos](https://portal.inmet.gov.br/uploads/dadoshistoricos/)