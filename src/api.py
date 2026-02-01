import os
from typing import Dict, Any, Optional

import pandas as pd
import uvicorn
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# --- Configuração da Aplicação ---
app = FastAPI(
    title="API Intuitive Care",
    description="API para consulta de despesas de operadoras de saúde (Teste Técnico).",
    version="1.0.0"
)

# Configuração de CORS (Permite acesso do Frontend Vue.js)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Constantes
DATA_PATH = os.path.join("data", "despesas_agregadas.csv")

# --- Funções Auxiliares ---

def get_data() -> pd.DataFrame:
    """
    Carrega os dados consolidados do CSV.
    Se o arquivo não existir, retorna um DataFrame de exemplo (Mock) para evitar falhas.
    
    Returns:
        pd.DataFrame: DataFrame contendo as despesas por operadora.
    """
    if not os.path.exists(DATA_PATH):
        # Mock de segurança caso o ETL não tenha sido executado
        return pd.DataFrame([
            {"Razao_Social": "UNIMED MOCK", "UF": "SP", "Total_Despesas": 500000.00, "RegistroANS": "123"},
            {"Razao_Social": "BRADESCO MOCK", "UF": "RJ", "Total_Despesas": 900000.00, "RegistroANS": "456"}
        ])
    
    try:
        df = pd.read_csv(DATA_PATH)
        
        # Tratamento crítico: JSON não aceita NaN.
        # Substituímos valores nulos por 0 ou string vazia para garantir integridade da resposta.
        df = df.fillna(0)
        return df
    except Exception as e:
        print(f"Erro ao ler CSV: {e}")
        return pd.DataFrame()

# --- Endpoints ---

@app.get("/api/operadoras", response_model=Dict[str, Any])
def list_operadoras(
    page: int = Query(1, ge=1, description="Número da página (inicia em 1)"), 
    limit: int = Query(10, ge=1, le=100, description="Itens por página (máx 100)"), 
    search: Optional[str] = Query(None, description="Busca textual por Razão Social")
) -> Dict[str, Any]:
    """
    Lista as operadoras com paginação e filtro de busca.
    
    - **page**: Número da página atual.
    - **limit**: Quantidade de registros por página.
    - **search**: Termo para filtrar por Razão Social (case-insensitive).
    """
    df = get_data()
    
    # Filtragem
    if search:
        # Garante conversão para string para evitar erros se a coluna tiver números misturados
        mask = df['Razao_Social'].astype(str).str.contains(search, case=False, na=False)
        df = df[mask]
    
    # Paginação
    total_records = len(df)
    start = (page - 1) * limit
    end = start + limit
    
    # Slice do DataFrame e conversão para lista de dicionários
    data = df.iloc[start:end].to_dict(orient="records")
    
    return {
        "data": data,
        "meta": {
            "total": total_records,
            "page": page,
            "limit": limit,
            "pages_total": (total_records // limit) + (1 if total_records % limit > 0 else 0)
        }
    }

@app.get("/api/estatisticas", response_model=Dict[str, Any])
def get_stats() -> Dict[str, Any]:
    """
    Retorna estatísticas agregadas para alimentar dashboards.
    
    Returns:
        - total_geral: Soma de todas as despesas.
        - distribuicao_uf: Dicionário com total de despesas por estado.
    """
    df = get_data()
    
    if df.empty:
        return {"total_geral": 0.0, "distribuicao_uf": {}}

    # Cálculo seguro do total (convertendo para float nativo do Python)
    total_geral = float(df["Total_Despesas"].sum())
    
    # Agregação por UF
    # Sort values garante que o gráfico mostre os maiores estados primeiro
    por_uf = df.groupby("UF")["Total_Despesas"].sum().sort_values(ascending=False).to_dict()
    
    return {
        "total_geral": total_geral,
        "distribuicao_uf": por_uf
    }

if __name__ == "__main__":
    # Executa o servidor de desenvolvimento
    uvicorn.run(app, host="0.0.0.0", port=8000)