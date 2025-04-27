import streamlit as st
import pandas as pd
import duckdb
from faker import Faker
import io

# Configuración
fake = Faker()

st.title("Mini consola SQL Server-like")

# Opción: subir o generar
modo = st.radio("¿Cómo quieres cargar los datos?", ("Subir CSV", "Generar datos inventados"))

# Cargar los datos
if modo == "Subir CSV":
    uploaded_file = st.file_uploader("Sube tu archivo CSV", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
else:
    # Configurar datos inventados
    n_rows = st.number_input("¿Cuántas filas quieres generar?", 10, 10000, step=10)
    columnas = st.multiselect(
        "¿Qué columnas quieres generar?",
        ["Nombre", "Apellido", "Email", "Fecha de nacimiento", "Dirección", "Empresa"],
        default=["Nombre", "Email"]
    )
    
    # Generar
    data = {}
    if "Nombre" in columnas:
        data["nombre"] = [fake.first_name() for _ in range(n_rows)]
    if "Apellido" in columnas:
        data["apellido"] = [fake.last_name() for _ in range(n_rows)]
    if "Email" in columnas:
        data["email"] = [fake.email() for _ in range(n_rows)]
    if "Fecha de nacimiento" in columnas:
        data["fecha_nacimiento"] = [fake.date_of_birth().strftime('%Y-%m-%d') for _ in range(n_rows)]
    if "Dirección" in columnas:
        data["direccion"] = [fake.address().replace("\n", ", ") for _ in range(n_rows)]
    if "Empresa" in columnas:
        data["empresa"] = [fake.company() for _ in range(n_rows)]
    
    df = pd.DataFrame(data)

# Mostrar los datos
if 'df' in locals():
    st.subheader("Datos cargados:")
    st.dataframe(df)

    # --- Botón para descargar archivo CSV ---
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button(
        label="Descargar datos como CSV",
        data=csv_buffer.getvalue(),
        file_name="datos_generados.csv",
        mime="text/csv"
    )

    # Crear conexión DuckDB en memoria
    con = duckdb.connect(database=':memory:')
    con.register('datos', df)

    # Entrada de consulta
    query = st.text_area("Escribe tu consulta SQL:", "SELECT * FROM datos LIMIT 10")

    if st.button("Ejecutar consulta"):
        try:
            result = con.execute(query).df()
            st.success("Consulta ejecutada correctamente.")
            st.dataframe(result)
        except Exception as e:
            st.error(f"Error en la consulta: {e}")
