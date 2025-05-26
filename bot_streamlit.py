import streamlit as st
import openai
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import httpx  # Importa httpx para manejar las conexiones
import sqlparse  # Importar sqlparse
import re
import pandas as pd

openai_client = openai.OpenAI(
    api_key=st.secrets["openai_api_key"]
)

# Configurar Google Cloud BigQuery usando st.secrets
gcp_credentials_json = st.secrets["google_application_credentials"]
gcp_credentials_dict = json.loads(gcp_credentials_json)

credentials = service_account.Credentials.from_service_account_info(gcp_credentials_dict)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# Título
st.title("Chatbot - Consulta datos de BigQuery")
pregunta = st.text_input("Haz tu pregunta sobre la tabla consolidada:")
pregunta_lower = pregunta.lower()  # 🔥 Añadido para evitar errores


if st.button("Enviar"):
    if pregunta:
        with st.spinner("Generando respuesta..."):
            prompt = f"""
            Tengo una tabla llamada `ft_ada_lovelance.formulado_consolidado_2024_2025` con columnas como LOTE, ADICION, CODIGO MP, INGREDIENT NAME, Columna, Valor, Fecha de producci_x0, Fecha de vencimiento, Lote_dim, Title, Producto.
            - LOTE (STRING): lote de la Materia Prima de la Dieta
            - ADICION (STRING)
            - CODIGO MP (STRING): código único de la Materia Prima de la Dieta
            - INGREDIENT NAME (STRING): nombre de la Materia Prima de la Dieta
            - Columna (STRING): código de la Dieta o Producto
            - Valor (FLOAT): cantidad de materia prima en la dieta
            - Title (STRING): código alternativo de la Dieta o Producto
            - Producto (STRING): nombre de la Dieta o Producto
            - Fecha de producci_x0 (DATE): fecha de producción de la Dieta
            - Fecha de vencimiento (DATE): fecha de vencimiento de la Dieta
            - Lote_dim (STRING)
            - Empresa (STRING)

                       
            Reglas para la generación de consultas:
           ✅ Si la pregunta menciona una Materia Prima por nombre (`INGREDIENT NAME`), genera una consulta SQL que busque todas las coincidencias usando un filtro inclusivo:
            - Transforma tanto `INGREDIENT NAME` como el texto de búsqueda a minúsculas con `LOWER()` para asegurar coincidencias insensibles a mayúsculas/minúsculas.
            - Usa `LIKE '%nombre%'` (inclusivo) para encontrar todas las apariciones parciales.
            - Devuelve los campos:
                - `Columna` (código de la dieta)
                - `Producto` (nombre de la dieta)
                - `Empresa` (Empresa que hizo la dieta)
                - `CODIGO MP` (código único de la materia prima)
                - `INGREDIENT NAME` (nombre completo de la materia prima encontrada)
                - `Fecha de producci_x0` (fecha de producción de la Dieta)
                - `Fecha de vencimiento` (fecha de vencimiento de la Dieta)

            Por ejemplo:
            ```sql
            SELECT DISTINCT `Columna`, `Producto`, `CODIGO MP`, `INGREDIENT NAME`
            FROM `ft_ada_lovelance.formulado_consolidado_2024_2025`
            WHERE LOWER(TRIM(`INGREDIENT NAME`)) LIKE LOWER('%nombre_a_buscar%')

            ✅ Si la pregunta menciona una Materia Prima (por `CODIGO MP` o `INGREDIENT NAME`), genera una consulta SQL que devuelva todas las Dietas (`Columna`) asociadas a esa Materia Prima.
            ✅ La consulta debe usar una única sentencia con un WHERE que combine condiciones usando OR, o bien usar UNION ALL entre SELECTs y un SELECT exterior con DISTINCT.
            ✅ Encierra siempre los nombres de columnas que tienen espacios entre backticks (`), por ejemplo: `CODIGO MP`.
            ✅ No uses alias directamente después de SELECT DISTINCT. Si necesitas alias, usa un subquery o colócalos en el SELECT exterior.
            ✅ Si la pregunta menciona específicamente una Dieta, devuelve solo la información de esa Dieta.
            ✅ Si la pregunta menciona "dame un código de dieta" o "dame el código de una dieta".
            ✅ Si se pregunta por fechas o nombres, devuelve un mensaje predeterminado cuando no haya resultados.
            genera una consulta SQL que devuelva el primer código de dieta (`Columna`) encontrado.
            No filtres por `Producto`, sino solo devuelve el primer resultado encontrado.

           🔎 Instrucciones adicionales:
            - Cuando se pregunte por "la fecha de vencimiento de la dieta X" (donde X puede ser cualquier nombre o código), y no se encuentre un registro (NULL o NaT), devuelve: "No se encontró fecha de vencimiento registrada."
            - Cuando se pregunte por "la fecha de producción de la dieta X" (donde X puede ser cualquier nombre o código), y no se encuentre un registro (NULL o NaT), devuelve: "No se encontró fecha de producción registrada."
            - Cuando se pregunte por "el nombre de la dieta o código X" (donde X puede ser cualquier código), y no se encuentre (NULL), devuelve: "No se encontró nombre registrado."
            - Si se pide el código de cualquier dieta y no se encuentra, devuelve: "No se encontró código de dieta registrado."
            - Si el usuario pregunta: "¿Qué dieta tiene la materia prima N24-013?", la consulta debe devolver tanto `Columna` como `Producto`, usando una única consulta (puede ser con OR o con UNION ALL y un SELECT exterior con DISTINCT).
            
            🔎 Instrucciones adicionales para búsqueda de países:
            ✅ Si el usuario menciona una frase (por ejemplo, "torta de soya boliviana"), **separa la frase en palabras clave** (por ejemplo: `torta`, `soya`, `boliviana`) y genera un WHERE como:
            ```sql
            WHERE LOWER(TRIM(`INGREDIENT NAME`)) LIKE '%torta%'
            AND LOWER(TRIM(`INGREDIENT NAME`)) LIKE '%soya%'
            AND LOWER(TRIM(`INGREDIENT NAME`)) LIKE '%bolivian%'

            🔎 Instrucciones adicionales:
            - Si la pregunta es como "¿qué materias primas tienen las dietas 451722 y 451723?" o "dame la fórmula de las dietas 451722 y 451723", la respuesta debe mostrar una tabla tipo pivot con las dietas como encabezados.
            - Una columna para cada dieta solicitada, con su código como encabezado y el nombre de la dieta a su lado, mostrando la cantidad (`Valor`) correspondiente.
            - La tabla debe contener: 
                - LOTE (STRING): lote de la Materia Prima de la Dieta
                - ADICION (STRING)
                - `CODIGO MP` (código único de la materia prima)
                - `INGREDIENT NAME` (nombre completo de la materia prima encontrada)
                - Valores por cada Dieta
         
            Genera una consulta SQL para BigQuery que responda esta pregunta: "{pregunta}".
            Usa el dataset y tabla `ft_ada_lovelance.formulado_consolidado_2024_2025`.
            """

            try:
                respuesta = openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "Eres un asistente experto en SQL para BigQuery."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2
                )
                sql_query = respuesta.choices[0].message.content.strip()

                # Extraer código SQL limpio
                if "```sql" in sql_query and "```" in sql_query:
                    sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
                sql_query = sql_query.encode('ascii', 'ignore').decode()

                # Validar y corregir el SELECT DISTINCT con alias
                if sql_query.strip().upper().startswith("SELECT DISTINCT"):
                    # Buscar el patrón SELECT DISTINCT columna AS alias
                    pattern = re.compile(r"SELECT\s+DISTINCT\s+`?(\w+)`?\s+AS\s+(\w+)", re.IGNORECASE)
                    match = pattern.search(sql_query)
                    if match:
                        columna = match.group(1)
                        sql_query = pattern.sub(f"SELECT DISTINCT `{columna}`", sql_query)
                        st.warning("⚠️ Se eliminó alias después de SELECT DISTINCT para BigQuery.")

                # Validaciones de SQL
                if sql_query.upper().count("SELECT") > 1 and "UNION" not in sql_query.upper():
                    st.error("❌ Error: La consulta tiene múltiples SELECT sin UNION. Revisa el prompt o consulta.")
                elif sql_query.strip().upper().startswith("SELECT DISTINCT") and " AS " in sql_query.upper():
                    st.warning("⚠️ Detectado alias en SELECT DISTINCT. Podría causar error en BigQuery.")
                
                else:
                    # Formatear y mostrar consulta
                    #sql_query_pretty = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
                    #st.subheader("Consulta generada:")
                    #st.code(sql_query_pretty, language='sql')

                    # Ejecutar consulta
                    query_job = client.query(sql_query)
                    resultados = query_job.result().to_dataframe()

                    # Supongamos que ya tienes resultados pivotados con códigos de dieta como columnas
                    # Aquí debes construir un diccionario para renombrar las columnas
                    columnas_originales = resultados.columns  # Aquí están los nombres originales (por ejemplo: '451722', '451723')
                    nombres_dieta = {
                        '451722': 'Aquaxcel',
                        '451723': 'Fórmula X'
                        # Añade más dietas y nombres si los tienes
                    }

                    # Construir nuevo diccionario de nombres
                    nuevos_nombres = {}
                    for col in columnas_originales:
                        if col in nombres_dieta:
                            nuevos_nombres[col] = f"{col}\n{nombres_dieta[col]}"
                        else:
                            nuevos_nombres[col] = col  # Mantener otros nombres sin cambio

                    # Renombrar columnas
                    resultados_formateados = resultados.rename(columns=nuevos_nombres)


                    st.subheader("Respuesta:")
                    if not resultados.empty:
                        pregunta_lower = pregunta.lower()
                        if resultados.shape[1] == 1:
                            # Solo una columna (como código, nombre, o fecha)
                            valor = resultados.iloc[0, 0]
                            if pd.api.types.is_datetime64_any_dtype(resultados.iloc[:, 0]):
                                if pd.isna(valor):
                                    if "fecha de vencimiento" in pregunta_lower:
                                        st.markdown("💬 **No se encontró fecha de vencimiento registrada.**")
                                    elif "fecha de produccion" in pregunta_lower or "fecha de producción" in pregunta_lower:
                                        st.markdown("💬 **No se encontró fecha de producción registrada.**")
                                    else:
                                        st.markdown("💬 **No se encontró fecha registrada.**")
                                else:
                                    st.markdown(f"💬 **La fecha es:** {valor.strftime('%Y-%m-%d')}")
                            elif isinstance(valor, str):
                                if not valor.strip():
                                    if "nombre" in pregunta_lower:
                                        st.markdown("💬 **No se encontró nombre registrado.**")
                                    elif "código" in pregunta_lower or "codigo" in pregunta_lower:
                                        st.markdown("💬 **No se encontró código de dieta registrado.**")
                                    else:
                                        st.markdown("💬 **No se encontró información registrada.**")
                                else:
                                    st.markdown(f"💬 **El resultado es:** {valor}")
                            else:
                                st.markdown(f"💬 **El resultado es:** {valor}")
                        else:
                            # Varias columnas (como Columna y Producto)
                            # Mostrar resultados en formato tabla con renombre de columna
                            resultados_formateados = resultados.rename(columns={
                                'Columna': 'Código de Dieta',
                                'Producto': 'Nombre',
                                'CODIGO MP': 'Código MP',
                                'INGREDIENT NAME': 'Nombre MP',
                                'Fecha de producci_x0': 'Fecha de Producción',
                                'Fecha de vencimiento': 'Fecha de Vencimiento'
                            })
                            st.subheader("📊 Tabla de Resultados")
                            # Usar st.markdown + st.dataframe con configuración CSS para ancho y wrap
                            st.markdown(
                                """
                                <style>
                                /* Hacer que las celdas de la tabla sean más anchas */
                                .css-1f4bu6p .dataframe {
                                    width: 100% !important;
                                }
                                /* Ajustar texto de celdas a múltiples líneas */
                                .css-1f4bu6p .dataframe td {
                                    white-space: normal !important;
                                    word-wrap: break-word !important;
                                }
                                </style>
                                """,
                                unsafe_allow_html=True
                            )

                            # Mostrar tabla con ajuste de texto
                            # Suponiendo que resultados es tu DataFrame final
                            resultados_formateados.index = resultados_formateados.index + 1  # Esto hace que el índice empiece desde 1
                            st.dataframe(resultados_formateados, use_container_width=True)

                    else:
                        # Mensajes según el contexto
                        if "fecha de vencimiento" in pregunta_lower:
                            st.markdown("💬 **No se encontró fecha de vencimiento registrada.**")
                        elif "fecha de produccion" in pregunta_lower or "fecha de producción" in pregunta_lower:
                            st.markdown("💬 **No se encontró fecha de producción registrada.**")
                        elif "nombre" in pregunta_lower:
                            st.markdown("💬 **No se encontró nombre registrado.**")
                        elif "código" in pregunta_lower or "codigo" in pregunta_lower:
                            st.markdown("💬 **No se encontró código de dieta registrado.**")
                        else:
                            st.info("No se encontraron resultados.")


            except Exception as e:
                st.error(f"❌ Error: {e}")
    else:
        st.warning("Por favor, escribe una pregunta.")
