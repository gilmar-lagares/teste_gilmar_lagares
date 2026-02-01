"""
Pipeline ETL - Teste Técnico Intuitive Care
-------------------------------------------
Responsável por:
1. Scraping e download de dados contábeis da ANS (Gov.br).
2. Download de dados cadastrais de operadoras (CADOP).
3. Validação de Dados (CNPJ, Valores).
4. Transformação de Dados (Limpeza, Normalização, Enriquecimento).
5. Agregação e Cálculo Estatístico (Média, Desvio Padrão).
"""

import os
import re
import warnings
import zipfile
from io import BytesIO
from typing import List, Optional, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Configurações e Constantes ---

# Ignorar avisos de SSL inseguro (comum em sites .gov.br)
warnings.filterwarnings("ignore")

URL_DEMONSTRACOES = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
URL_CADOP_DIR = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
DATA_DIR = "data"
TIMEOUT = 60  # Segundos

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Garante que a pasta de dados existe
os.makedirs(DATA_DIR, exist_ok=True)


# --- Lógica de Negócio: Validação ---

def validar_cnpj(cnpj: Any) -> bool:
    """
    Valida um CNPJ usando o algoritmo de Módulo 11 (dígitos verificadores).
    
    Args:
        cnpj: O CNPJ a ser validado (string ou int).
        
    Returns:
        bool: True se válido, False se inválido.
    """
    # Remove caracteres não numéricos
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    
    # Verifica tamanho e se todos os dígitos são iguais (ex: 11111111111111)
    if len(cnpj_limpo) != 14 or len(set(cnpj_limpo)) == 1:
        return False

    # Validação do 1º Dígito
    pesos = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(cnpj_limpo[i]) * pesos[i] for i in range(12))
    resto = soma % 11
    digito1 = 0 if resto < 2 else 11 - resto

    if int(cnpj_limpo[12]) != digito1:
        return False

    # Validação do 2º Dígito
    pesos.insert(0, 6)
    soma = sum(int(cnpj_limpo[i]) * pesos[i] for i in range(13))
    resto = soma % 11
    digito2 = 0 if resto < 2 else 11 - resto

    return int(cnpj_limpo[13]) == digito2


# --- Infraestrutura: Scraping e Download ---

def listar_links_na_pagina(url: str, extensao: str) -> List[str]:
    """
    Faz scraping de uma página para encontrar links com uma extensão específica.
    
    Args:
        url: URL alvo.
        extensao: Extensão do arquivo para filtrar (ex: '.zip', '.csv').
        
    Returns:
        List[str]: Lista de URLs completas encontradas.
    """
    try:
        response = requests.get(url, headers=HEADERS, verify=False, timeout=30)
        if response.status_code != 200:
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for a in soup.find_all('a'):
            href = a.get('href')
            if href and href.lower().endswith(extensao.lower()):
                # Trata URLs relativas vs absolutas
                link_completo = (url + href) if not href.startswith('http') else href
                links.append(link_completo)
        return links
    except Exception as e:
        print(f"Aviso: Falha ao ler {url}: {e}")
        return []

def baixar_cadop() -> Optional[pd.DataFrame]:
    """
    Localiza e baixa o CSV de Cadastro de Operadoras (CADOP) para enriquecimento.
    
    Returns:
        pd.DataFrame: DataFrame com colunas normalizadas, ou None em caso de falha.
    """
    print(">>> 1. [Extração] Buscando Dados do CADOP (Metadados)...")
    try:
        links_csv = listar_links_na_pagina(URL_CADOP_DIR, '.csv')
        
        # Heurística para achar o arquivo certo (geralmente contém 'relatorio' ou 'cadop')
        link_correto = next(
            (l for l in links_csv if 'relatorio' in l.lower() or 'cadop' in l.lower()), 
            links_csv[0] if links_csv else None
        )

        if link_correto:
            # Baixa e normaliza colunas imediatamente
            df = pd.read_csv(link_correto, sep=';', encoding='latin1', dtype=str, on_bad_lines='skip')
            df.columns = [c.strip().upper() for c in df.columns]
            return df
        return None
    except Exception as e:
        print(f"Erro ao baixar CADOP: {e}")
        return None

def baixar_demonstracoes() -> List[str]:
    """
    Navega nos diretórios da ANS para encontrar e baixar ZIPs contábeis 
    dos últimos 3 anos disponíveis.
    
    Returns:
        List[str]: Lista de caminhos locais dos CSVs extraídos.
    """
    print(">>> 2. [Extração] Buscando Demonstrações Contábeis (Últimos 3 Anos)...")
    arquivos_validos = []
    
    try:
        # Pega lista de anos disponíveis
        resp = requests.get(URL_DEMONSTRACOES, headers=HEADERS, verify=False, timeout=30)
        soup = BeautifulSoup(resp.text, 'html.parser')
        # Filtra links que parecem anos (4 dígitos)
        anos = sorted(
            [a.get('href') for a in soup.find_all('a') if a.get('href') and a.get('href').strip('/').isdigit()], 
            reverse=True
        )[:3]
    except Exception as e:
        print(f"Erro ao acessar raiz: {e}")
        return []

    # Itera sobre os anos e baixa os ZIPs
    for ano in anos:
        if len(arquivos_validos) >= 3: break
        
        url_ano = URL_DEMONSTRACOES + ano
        zips = sorted(listar_links_na_pagina(url_ano, '.zip'), reverse=True)
        
        for link_zip in zips:
            if len(arquivos_validos) >= 3: break
            try:
                r = requests.get(link_zip, headers=HEADERS, verify=False, timeout=TIMEOUT)
                if r.status_code == 200:
                    with zipfile.ZipFile(BytesIO(r.content)) as z:
                        # Encontra o maior CSV dentro do zip (assume-se que é o arquivo de dados principal)
                        csvs = sorted(
                            [f for f in z.namelist() if f.lower().endswith('.csv')], 
                            key=lambda x: z.getinfo(x).file_size, 
                            reverse=True
                        )
                        if csvs:
                            z.extract(csvs[0], DATA_DIR)
                            caminho_local = os.path.join(DATA_DIR, csvs[0])
                            arquivos_validos.append(caminho_local)
                            print(f"   -> Baixado e Extraído: {csvs[0]}")
            except Exception as e:
                print(f"   -> Erro ao processar zip {link_zip}: {e}")
                
    return arquivos_validos


# --- Orquestração do Pipeline ---

def pipeline_principal():
    """
    Fluxo principal de execução do ETL:
    1. Extração (CADOP e Contábil).
    2. Transformação (Limpeza, Validação, Join).
    3. Carga (Salvar CSV/ZIP).
    4. Análise (Cálculo Estatístico).
    """
    # --- Passo 1: Extração ---
    df_cadop = baixar_cadop()
    mapa_cadop = {}
    
    # Pré-processa CADOP em um Hash Map para busca O(1)
    if df_cadop is not None:
        # Busca dinâmica de colunas
        cols = {k: next((c for c in df_cadop.columns if k in c), None) for k in ['REGISTRO', 'CNPJ', 'RAZAO', 'UF', 'MODALIDADE']}
        
        for _, row in df_cadop.iterrows():
            reg = str(row[cols['REGISTRO']]).strip()
            if reg:
                mapa_cadop[reg] = {
                    'CNPJ': row[cols['CNPJ']],
                    'RazaoSocial': row[cols['RAZAO']],
                    'UF': row[cols['UF']],
                    'Modalidade': row[cols['MODALIDADE']]
                }

    arquivos = baixar_demonstracoes()
    if not arquivos:
        print("Erro Crítico: Nenhum arquivo de dados foi baixado.")
        return

    # --- Passo 2: Transformação ---
    dfs = []
    print(">>> 3. [Transformação] Limpando, Validando e Enriquecendo...")
    
    for arq in arquivos:
        try:
            # Leitura resiliente a encoding e separador
            df = pd.read_csv(arq, sep=';', encoding='latin1', dtype=str, on_bad_lines='skip')
            df.columns = [c.strip().upper() for c in df.columns]
            
            # Normalizar nomes das colunas
            mapa_cols = {'REG_ANS': 'RegistroANS', 'CD_CONTA_CONTABIL': 'Conta', 'VL_SALDO_FINAL': 'Valor', 'DATA': 'Data'}
            df.rename(columns=lambda x: mapa_cols.get(x, x), inplace=True)
            
            if 'Valor' in df.columns:
                # Conversão Numérica (Trata formato de moeda brasileiro)
                df['Valor'] = df['Valor'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0)
                
                # Enriquecimento (Join com CADOP via Hash Map)
                df['RegistroANS'] = df['RegistroANS'].str.strip()
                
                # Aplica o mapa para buscar dados cadastrais
                df['CNPJ'] = df['RegistroANS'].apply(lambda x: mapa_cadop.get(x, {}).get('CNPJ', '00000000000000'))
                df['RazaoSocial'] = df['RegistroANS'].apply(lambda x: mapa_cadop.get(x, {}).get('RazaoSocial', 'DESCONHECIDO'))
                df['UF'] = df['RegistroANS'].apply(lambda x: mapa_cadop.get(x, {}).get('UF', 'ND'))
                df['Modalidade'] = df['RegistroANS'].apply(lambda x: mapa_cadop.get(x, {}).get('Modalidade', 'ND'))
                
                # --- Validações (Requisito 2.1) ---
                
                # 1. Validação de CNPJ
                df['CNPJ_Valido'] = df['CNPJ'].apply(validar_cnpj)
                
                # 2. Razão Social Não Vazia
                df = df[df['RazaoSocial'] != 'DESCONHECIDO'] 
                
                # 3. Valores Positivos (Apenas despesas reais, ignora estornos/negativos para análise)
                df = df[df['Valor'] > 0] 

                # Seleção Final de Colunas
                cols_finais = ['CNPJ', 'RazaoSocial', 'RegistroANS', 'Modalidade', 'UF', 'Valor']
                dfs.append(df[[c for c in cols_finais if c in df.columns]])
                
        except Exception as e:
            print(f"Erro ao processar arquivo {arq}: {e}")

    # --- Passo 3: Carga e Análise ---
    if dfs:
        df_consolidado = pd.concat(dfs, ignore_index=True)
        
        # Salvar CSV Consolidado (Requisito 1.3)
        caminho_csv = os.path.join(DATA_DIR, "consolidado_despesas.csv")
        df_consolidado.to_csv(caminho_csv, index=False)
        
        # Salvar ZIP Final (Requisito 1.3 Entrega)
        with zipfile.ZipFile(os.path.join(DATA_DIR, "consolidado_despesas.zip"), "w") as z:
            z.write(caminho_csv, arcname="consolidado_despesas.csv")
            
        print(">>> 4. [Análise] Gerando Estatísticas Agregadas (Média/Desvio Padrão)...")
        
        # Agrupamento por Operadora e UF
        agregado = df_consolidado.groupby(['RazaoSocial', 'UF'])['Valor'].agg(
            Total_Despesas='sum',
            Media_Trimestral='mean',  # Desafio Adicional: Média
            Desvio_Padrao='std'       # Desafio Adicional: Desvio Padrão
        ).reset_index()
        
        # Trata NaN no desvio padrão (caso de registro único)
        agregado['Desvio_Padrao'] = agregado['Desvio_Padrao'].fillna(0)
        
        # Ordenação por maior despesa (Pareto)
        agregado.sort_values('Total_Despesas', ascending=False, inplace=True)
        
        # Ajuste de nome para API (RazaoSocial -> Razao_Social)
        agregado.rename(columns={'RazaoSocial': 'Razao_Social'}, inplace=True)
        
        # Salva arquivo otimizado para a API/Frontend
        caminho_agg = os.path.join(DATA_DIR, "despesas_agregadas.csv")
        agregado.to_csv(caminho_agg, index=False)
        
        print(f">>> SUCESSO! Pipeline concluído. Arquivos gerados em '{DATA_DIR}'.")
    else:
        print("Aviso: Nenhum dado foi processado com sucesso.")

if __name__ == "__main__":
    pipeline_principal()