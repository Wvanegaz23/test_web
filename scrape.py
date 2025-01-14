from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException
from datetime import timedelta
from pathlib import Path



# Crear un directorio llamado 'data'
data_dir = Path('data')
data_dir.mkdir(exist_ok=True, parents=True)


def setup_chrome_driver():
    """
    Configura el driver de Chrome con manejo de errores mejorado
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    try:
        driver_path = ChromeDriverManager().install()
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except WebDriverException as e:
        print(f"Error al inicializar ChromeDriver: {e}")
        try:
            driver = webdriver.Chrome(options=chrome_options)
            return driver
        except Exception as e2:
            print(f"Error en método alternativo: {e2}")
            return None


def scrape_page(driver, url, wait):
    """
    Hace scraping de una página específica
    """
    print(f"Accediendo a: {url}")
    driver.get(url)

    # Esperar a que los productos se carguen
    try:
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "styles_name__qQJiK")))
    except TimeoutException:
        print(f"Timeout esperando productos en {url}")
        return [], []

    # Hacer scroll para asegurar que todos los productos se carguen
    for _ in range(2):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    # Extraer información
    items_nombres = driver.find_elements(By.CLASS_NAME, "styles_name__qQJiK")
    items_precios = driver.find_elements(By.CLASS_NAME, "ProductPrice_container__price__XmMWA")
    items_priceUnd = driver.find_elements(By.CLASS_NAME, "product-unit_price-unit__text__qeheS ")# Encuentra los elementos con la clase especificada

    productos = [nombre.text.strip() for nombre in items_nombres]
    precios = [precio.text.strip() for precio in items_precios]
    priceUnds = [priceUnd.text.strip() for priceUnd in items_priceUnd]

    print(f"Encontrados {len(productos)} productos en esta página")
    return productos, precios, priceUnds

def scrape_exito_products(max_pages=5):
    """
    Función para hacer scraping de productos de la sección de despensa del Éxito
    Args:
        max_pages (int): Número máximo de páginas a scrapear
    Returns:
        pandas.DataFrame: DataFrame con la información de los productos
    """
    driver = setup_chrome_driver()
    if driver is None:
        print("No se pudo inicializar el navegador.")
        return None

    try:
        wait = WebDriverWait(driver, 20)
        all_productos = []
        all_precios = []
        all_priceUnd = []

        # Scrapear cada página
        for page in range(max_pages):
            if page == 0:
                url = "https://www.exito.com/mercado/despensa?"
            else:
                url = f"https://www.exito.com/mercado/despensa?category-1=mercado&category-2=despensa&facets=category-1%2Ccategory-2&sort=score_desc&page={page}"

            print(f"\nScraping página {page + 1}")
            productos, precios, priceUnds = scrape_page(driver, url, wait)

            # Si no hay productos, asumimos que llegamos al final
            if not productos:
                print(f"No se encontraron productos en la página {page + 1}. Finalizando...")
                break

            all_productos.extend(productos)
            all_precios.extend(precios)
            all_priceUnd.extend(priceUnds)

            # Pequeña pausa entre páginas
            time.sleep(2)

        # Crear DataFrame con todos los productos
        df = pd.DataFrame({
            'Producto': all_productos,
            'Precio': all_precios,
            'Precio_Unidad': all_priceUnd
        })

        # Aplicar las funciones a la columna 'Producto' y crear nuevas columnas 'Peso', 'Categoria' y 'Marca'
        df['Peso'] = df['Producto'].apply(extraer_peso)
        df['Categoria'] = df['Producto'].apply(extraer_categoria)
        df['Marca'] = df['Producto'].apply(extraer_marca)
        df['Fecha'] = pd.to_datetime('today').strftime('%Y-%m-%d')

        print(f"\nTotal de productos encontrados: {len(df)}")
        return df

    except Exception as e:
        print(f"Error durante el scraping: {e}")
        return None

    finally:
        print("Cerrando el navegador...")
        driver.quit()

    # Función para extraer el peso de la columna 'Producto'
def extraer_peso(producto):
    match = re.search(r'\((.*?)\)', producto)
    if match:
        return match.group(1)
    return None

# Función para extraer la primera palabra de la columna 'Producto'
def extraer_categoria(producto):
    return producto.split()[0]

# Función para extraer las palabras en mayúsculas de la columna 'Producto'
def extraer_marca(producto):
    return ' '.join([word for word in producto.split() if word.isupper()])


def guardar_resultados(df, nombre_archivo='data/productos_exito.csv'):
    """
    Guarda los resultados en un archivo CSV, agregando los datos al archivo existente si ya existe
    """
    if df is not None and not df.empty:
        try:
            # Leer el archivo existente si existe
            df_existente = pd.read_csv(nombre_archivo, encoding='utf-8-sig')
            # Concatenar los nuevos datos con los existentes
            df = pd.concat([df_existente, df], ignore_index=True)
        except FileNotFoundError:
            # Si el archivo no existe, simplemente guardamos el nuevo DataFrame
            pass

        df.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
        print(f"Resultados guardados exitosamente en {nombre_archivo}")
        print(f"Se guardaron {len(df)} productos")
    else:
        print("No hay datos para guardar")

if __name__ == "__main__":
    print("Iniciando proceso de scraping...")
    # Puedes ajustar el número máximo de páginas aquí
    df_productos = scrape_exito_products(max_pages=28)
    guardar_resultados(df_productos)
    print("Proceso de scraping finalizado")
