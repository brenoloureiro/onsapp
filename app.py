import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import clickhouse_connect
from datetime import datetime, timedelta

# Configuração da página
st.set_page_config(
    page_title="Dashboard ONS - Balanço Energético",
    page_icon="⚡",
    layout="wide"
)

# Cache da conexão ClickHouse
@st.cache_resource
def init_clickhouse_connection():
    """Inicializa conexão com ClickHouse Cloud"""
    try:
        client = clickhouse_connect.get_client(
            host=st.secrets["CLICKHOUSE_HOST"],
            port=st.secrets.get("CLICKHOUSE_PORT", 8443),
            username=st.secrets["CLICKHOUSE_USER"],
            password=st.secrets["CLICKHOUSE_PASSWORD"],
            secure=True
        )
        return client
    except Exception as e:
        st.error(f"Erro na conexão com ClickHouse: {e}")
        st.info("Configure as credenciais em .streamlit/secrets.toml")
        return None

# Cache de dados
@st.cache_data(ttl=300)  # Cache por 5 minutos
def load_data(_client, query: str) -> pd.DataFrame:
    """Executa query e retorna DataFrame"""
    try:
        return _client.query_df(query)
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        return pd.DataFrame()

def main():
    # Título
    st.title("⚡ Dashboard Balanço Energético ONS")
    st.markdown("**Sistema Elétrico Brasileiro - Dados Semi-horários**")
    
    # Inicializar conexão
    client = init_clickhouse_connection()
    if not client:
        st.stop()
    
    # Sidebar - Controles
    st.sidebar.header("📊 Filtros")
    
    # Seleção de período (últimos 7 dias por padrão)
    data_fim = datetime.now().date()
    data_inicio = data_fim - timedelta(days=7)
    
    periodo = st.sidebar.date_input(
        "Período de Análise:",
        value=(data_inicio, data_fim),
        format="DD/MM/YYYY"
    )
    
    if len(periodo) == 2:
        data_inicio, data_fim = periodo
    else:
        data_inicio = data_fim = periodo[0]
    
    # Seleção de subsistemas
    subsistemas_query = """
    SELECT DISTINCT nom_subsistema 
    FROM balanco_energia_subsistemas 
    WHERE nom_subsistema != '' 
    ORDER BY nom_subsistema
    """
    
    df_subsistemas = load_data(client, subsistemas_query)
    
    if not df_subsistemas.empty:
        subsistemas_disponíveis = df_subsistemas['nom_subsistema'].tolist()
        subsistemas_selecionados = st.sidebar.multiselect(
            "Subsistemas:",
            options=subsistemas_disponíveis,
            default=subsistemas_disponíveis[:3] if len(subsistemas_disponíveis) > 3 else subsistemas_disponíveis
        )
    else:
        st.error("Não foi possível carregar os subsistemas")
        st.stop()
    
    # Query principal para o gráfico
    if subsistemas_selecionados:
        subsistemas_str = "', '".join(subsistemas_selecionados)
        
        query_principal = f"""
        SELECT 
            din_instante,
            nom_subsistema,
            val_carga,
            COALESCE(val_gerhidraulica, 0) as geracao_hidraulica,
            COALESCE(val_gertermica, 0) as geracao_termica,
            COALESCE(val_gereolica, 0) as geracao_eolica,
            COALESCE(val_gersolar, 0) as geracao_solar,
            (COALESCE(val_gerhidraulica, 0) + COALESCE(val_gertermica, 0) + 
             COALESCE(val_gereolica, 0) + COALESCE(val_gersolar, 0)) as geracao_total
        FROM balanco_energia_subsistemas 
        WHERE 
            data_ref >= '{data_inicio}' 
            AND data_ref <= '{data_fim}'
            AND nom_subsistema IN ('{subsistemas_str}')
        ORDER BY din_instante, nom_subsistema
        """
        
        # Carregar dados
        df = load_data(client, query_principal)
        
        if df.empty:
            st.warning("Nenhum dado encontrado para o período selecionado")
            st.stop()
        
        # Converter datetime
        df['din_instante'] = pd.to_datetime(df['din_instante'])
        
        # Métricas principais
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            carga_total = df['val_carga'].sum() / 2  # Semi-horário para MWh
            st.metric("Energia Total", f"{carga_total:,.0f} MWh")
        
        with col2:
            demanda_media = df['val_carga'].mean()
            st.metric("Demanda Média", f"{demanda_media:,.0f} MW")
        
        with col3:
            pico_demanda = df['val_carga'].max()
            st.metric("Pico de Demanda", f"{pico_demanda:,.0f} MW")
        
        with col4:
            geracao_renovavel = (df['geracao_hidraulica'] + df['geracao_eolica'] + df['geracao_solar']).sum()
            geracao_total_soma = df['geracao_total'].sum()
            renovavel_pct = (geracao_renovavel / geracao_total_soma * 100) if geracao_total_soma > 0 else 0
            st.metric("Renovável", f"{renovavel_pct:.1f}%")
        
        st.markdown("---")
        
        # GRÁFICO PRINCIPAL: Curva de Carga por Subsistema
        st.subheader("📈 Curva de Carga por Subsistema")
        
        fig = go.Figure()
        
        # Cores para cada subsistema
        cores = px.colors.qualitative.Set1
        
        for i, subsistema in enumerate(subsistemas_selecionados):
            df_sub = df[df['nom_subsistema'] == subsistema].copy()
            
            if not df_sub.empty:
                fig.add_trace(go.Scatter(
                    x=df_sub['din_instante'],
                    y=df_sub['val_carga'],
                    mode='lines',
                    name=subsistema,
                    line=dict(color=cores[i % len(cores)], width=2),
                    hovertemplate=f'<b>{subsistema}</b><br>' +
                                  'Data: %{x}<br>' +
                                  'Carga: %{y:,.0f} MW<extra></extra>'
                ))
        
        fig.update_layout(
            title="Demanda por Subsistema (MW)",
            xaxis_title="Data/Hora",
            yaxis_title="Carga (MW)",
            height=500,
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Configurações do eixo X
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        )
        
        # Configurações do eixo Y
        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Informações adicionais
        with st.expander("ℹ️ Informações dos Dados"):
            st.write(f"**Período:** {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")
            st.write(f"**Subsistemas:** {', '.join(subsistemas_selecionados)}")
            st.write(f"**Total de registros:** {len(df):,}")
            st.write(f"**Frequência:** Dados semi-horários")
            
            # Estatísticas por subsistema
            st.write("**Estatísticas por Subsistema:**")
            stats = df.groupby('nom_subsistema')['val_carga'].agg([
                'mean', 'max', 'min', 'std'
            ]).round(1)
            stats.columns = ['Média (MW)', 'Máximo (MW)', 'Mínimo (MW)', 'Desvio Padrão']
            st.dataframe(stats, use_container_width=True)
    
    else:
        st.warning("Selecione pelo menos um subsistema para visualizar os dados")

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            Dashboard ONS - Sistema Elétrico Brasileiro | Dados do ClickHouse Cloud
        </div>
        """, 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()