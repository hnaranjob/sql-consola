import streamlit as st
import pandas as pd
import duckdb
from faker import Faker
import io

# Configuración
fake = Faker()

# Lista para guardar consultas y resultados
consultas_guardadas = []
resultados_guardados = []

# Título
st.title("Mini Consola SQL en Streamlit")

# Selección de la semilla para generar datos replicables
semilla = st.number_input("Elige una semilla para replicar los datos:", min_value=1, max_value=10000, value=1234)

# Configurar la semilla
Faker.seed(semilla)

# Número de filas a generar
n_rows = st.number_input("¿Cuántas filas quieres generar?", 10, 1000, step=10)

# Función para generar la tabla de "Clientes y Consumo"
def generar_tabla_clientes_consumo(n, clientes, empresas):
    data = {
        "cliente_id": [fake.random_element(elements=clientes["cliente_id"].values) for _ in range(n)],  # Asignar un cliente aleatorio de la tabla de clientes
        "empresa": [fake.random_element(elements=empresas["empresa"].values) for _ in range(n)],  # Empresas repetidas
        "consumo_mes": [fake.random_number(digits=2) for _ in range(n)],
        "facturacion_mes": [fake.random_number(digits=5) for _ in range(n)],
        "coste_mes": [fake.random_number(digits=3) for _ in range(n)],
    }
    return pd.DataFrame(data)

# Función para generar la tabla de "Clientes Personales"
def generar_tabla_clientes_personales(n):
    data = {
        "cliente_id": [fake.uuid4() for _ in range(n)],  # Generar un ID único para cada cliente
        "cliente": [fake.name() for _ in range(n)],
        "nombre": [fake.first_name() for _ in range(n)],
        "apellido": [fake.last_name() for _ in range(n)],
        "fecha_nacimiento": [fake.date_of_birth().strftime('%Y-%m-%d') for _ in range(n)],
        "ciudad": [fake.city() for _ in range(n)],
    }
    return pd.DataFrame(data)

# Función para generar la tabla de "Empresas y Tarifas"
def generar_tabla_empresas_tarifas(n):
    data = {
        "empresa": [fake.company() for _ in range(n)],
        "tarifa": [fake.random_element(elements=('Básica', 'Intermedia', 'Premium')) for _ in range(n)],
        "trimestre": [fake.random_element(elements=('Q1', 'Q2', 'Q3', 'Q4')) for _ in range(n)],
    }
    return pd.DataFrame(data)

# Generar las tablas
df_clientes_personales = generar_tabla_clientes_personales(n_rows)
df_empresas_tarifas = generar_tabla_empresas_tarifas(n_rows)

# Los clientes de la tabla 1 deben ser los mismos que los de la tabla 2
df_clientes_consumo = generar_tabla_clientes_consumo(n_rows, df_clientes_personales, df_empresas_tarifas)

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
query = st.text_area("Escribe tu consulta SQL (usando JOIN):", 
                     "SELECT c.cliente, c.nombre, e.tarifa, c.consumo_mes FROM clientes_consumo c "
                     "JOIN clientes_personales c2 ON c.cliente_id = c2.cliente_id "
                     "JOIN empresas_tarifas e ON c.empresa = e.empresa LIMIT 10")

if st.button("Ejecutar consulta"):
    try:
        # Ejecutar consulta SQL
        result = con.execute(query).df()

        # Guardar consulta y resultado
        consultas_guardadas.append(query)
        resultados_guardados.append(result)

        # Mostrar resultado
        st.success("Consulta ejecutada correctamente.")
        st.dataframe(result)

    except Exception as e:
        st.error(f"Error en la consulta: {e}")

# Mostrar consultas guardadas
st.subheader("Consultas anteriores:")
for idx, (consulta, resultado) in enumerate(zip(consultas_guardadas, resultados_guardados)):
    st.write(f"**Consulta {idx + 1}:** {consulta}")
    st.dataframe(resultado)

    # Botón para borrar una consulta
    if st.button(f"Borrar consulta {idx + 1}"):
        consultas_guardadas.pop(idx)
        resultados_guardados.pop(idx)
        st.success(f"Consulta {idx + 1} borrada.")
        break  # Interrumpir el ciclo para evitar errores de índice
