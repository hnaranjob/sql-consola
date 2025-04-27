import streamlit as st
import pandas as pd
import duckdb
from faker import Faker
import io

# Configuración
fake = Faker()

# Lista para guardar las consultas (en memoria, sin base de datos)
if 'consultas_guardadas' not in st.session_state:
    st.session_state.consultas_guardadas = []

if 'resultados_guardados' not in st.session_state:
    st.session_state.resultados_guardados = []

# Título
st.title("Mini Consola SQL en Streamlit")

# Selección de la semilla para generar datos replicables
semilla = st.number_input("Elige una semilla para replicar los datos:", min_value=1, max_value=10000, value=1234)

# Configurar la semilla
Faker.seed(semilla)

# Número de clientes y empresas a generar
n_clientes = st.number_input("¿Cuántos clientes quieres generar?", 10, 1000, step=10)
n_empresas = st.number_input("¿Cuántas empresas quieres generar?", 1, 100, step=1)

# Función para generar la tabla de "Clientes y Consumo"
def generar_tabla_clientes_consumo(n_clientes, empresas):
    data = {
        "cliente_id": [i + 1 for i in range(n_clientes)],  # IDs consecutivos de clientes
        "empresa_id": [fake.random_int(min=1, max=n_empresas) for _ in range(n_clientes)],  # Empresas aleatorias entre las disponibles
        "consumo_mes": [fake.random_number(digits=2) for _ in range(n_clientes)],
        "facturacion_mes": [fake.random_number(digits=5) for _ in range(n_clientes)],
        "coste_mes": [fake.random_number(digits=3) for _ in range(n_clientes)],
    }
    return pd.DataFrame(data)

# Función para generar la tabla de "Clientes Personales"
def generar_tabla_clientes_personales(n_clientes):
    data = {
        "cliente_id": [i + 1 for i in range(n_clientes)],  # IDs consecutivos de clientes
        "nombre": [fake.first_name() for _ in range(n_clientes)],
        "apellido": [fake.last_name() for _ in range(n_clientes)],
        "fecha_nacimiento": [fake.date_of_birth().strftime('%Y-%m-%d') for _ in range(n_clientes)],
        "ciudad": [fake.city() for _ in range(n_clientes)],
    }
    return pd.DataFrame(data)

# Función para generar la tabla de "Empresas y Tarifas"
def generar_tabla_empresas_tarifas(n_empresas):
    data = {
        "empresa_id": [i + 1 for i in range(n_empresas)],  # IDs consecutivos de empresas
        "empresa": [fake.company() for _ in range(n_empresas)],
        "tarifa": [fake.random_number(digits=2) for _ in range(n_empresas)],  # Tarifa como valor numérico
        "trimestre": [fake.random_element(elements=('Q1', 'Q2', 'Q3', 'Q4')) for _ in range(n_empresas)],
    }
    return pd.DataFrame(data)

# Generar las tablas
df_clientes_personales = generar_tabla_clientes_personales(n_clientes)
df_empresas_tarifas = generar_tabla_empresas_tarifas(n_empresas)
df_clientes_consumo = generar_tabla_clientes_consumo(n_clientes, df_empresas_tarifas)

# Mostrar los datos generados
st.subheader("Datos generados:")
st.write("### Tabla: Clientes Personales")
st.dataframe(df_clientes_personales)
st.write("### Tabla: Empresas y Tarifas")
st.dataframe(df_empresas_tarifas)
st.write("### Tabla: Clientes y Consumo")
st.dataframe(df_clientes_consumo)

# Crear conexión DuckDB en memoria
con = duckdb.connect(database=':memory:')
con.register('clientes_personales', df_clientes_personales)
con.register('empresas_tarifas', df_empresas_tarifas)
con.register('clientes_consumo', df_clientes_consumo)

# Botón para descargar los datos generados
csv_buffer = io.StringIO()
df_clientes_personales.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue()
st.download_button(
    label="Descargar Tabla Clientes Personales como CSV",
    data=csv_data,
    file_name="clientes_personales.csv",
    mime="text/csv"
)

csv_buffer = io.StringIO()
df_empresas_tarifas.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue()
st.download_button(
    label="Descargar Tabla Empresas y Tarifas como CSV",
    data=csv_data,
    file_name="empresas_tarifas.csv",
    mime="text/csv"
)

csv_buffer = io.StringIO()
df_clientes_consumo.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue()
st.download_button(
    label="Descargar Tabla Clientes y Consumo como CSV",
    data=csv_data,
    file_name="clientes_consumo.csv",
    mime="text/csv"
)

# Entrada de consulta SQL
query = st.text_area("Escribe tu consulta SQL. Ejemplo: ", 
                     "SELECT cliente_id, nombre, fecha_nacimiento "
                     "FROM clientes_personales "
                     "LIMIT 10")

if st.button("Ejecutar consulta"):
    try:
        # Ejecutar consulta SQL
        result = con.execute(query).df()

        # Guardar consulta y resultado en el listado
        st.session_state.consultas_guardadas.append(query)
        st.session_state.resultados_guardados.append(result)

        # Mostrar resultado
        st.success("Consulta ejecutada correctamente.")
        st.dataframe(result)

    except Exception as e:
        st.error(f"Error en la consulta: {e}")

# Mostrar consultas guardadas
st.subheader("Consultas anteriores:")
for idx, consulta in enumerate(st.session_state.consultas_guardadas):
    st.write(f"**Consulta {idx + 1}:** {consulta}")
    
    # Recuperar el resultado de la consulta
    resultado = st.session_state.resultados_guardados[idx]
    st.dataframe(resultado)

    # Botón para borrar una consulta
    if st.button(f"Borrar consulta {idx + 1}"):
        # Eliminar la consulta y su resultado
        st.session_state.consultas_guardadas.pop(idx)
        st.session_state.resultados_guardados.pop(idx)
        st.success(f"Consulta {idx + 1} borrada.")
        break  # Interrumpir el ciclo para evitar errores de índice
