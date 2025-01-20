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
    items_priceProm = driver.find_elements(By.CLASS_NAME, "priceSection_container-promotion__RSQZO")
    # Extraer información - buscar enlaces dentro de los divs con la clase específica
    product_elements = driver.find_elements(By.CSS_SELECTOR, "div.productCard_productInfo__yn2lK a[data-testid='product-link']")

    productos = [nombre.text.strip() for nombre in items_nombres]
    precios = [precio.text.strip() for precio in items_precios]
    priceUnds = [priceUnd.text.strip() for priceUnd in items_priceUnd]
    priceProm = [priceProm.text.strip() for priceProm in items_priceProm]
    # Obtener los enlaces
    elements = []
    for element in product_elements:
      elements.append(element.get_attribute('href')) 

    print(f"Encontrados {len(productos)} productos en esta página")
    return productos, precios, priceUnds, priceProm,elements

def get_url_for_page(page):
    """
    Genera la URL correcta para cada página
    """
    base_url = "https://www.exito.com/mercado/despensa"
    category_params = "category-2=despensa&category-3=cafe-chocolate-y-cremas-no-lacteas&facets=category-2%2Ccategory-3&sort=score_desc"
    return f"{base_url}?{category_params}&page={page}"

def scrape_exito_products():
    """
    Función para hacer scraping de productos de la sección de despensa del Éxito
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
        all_priceProm = []
        all_elements = []
        
        page = 0
        productos_encontrados = True
        intentos_fallidos = 0
        MAX_INTENTOS_FALLIDOS = 2

        while productos_encontrados and intentos_fallidos < MAX_INTENTOS_FALLIDOS:
            url = get_url_for_page(page)
            print(f"\nScraping página {page + 1}: {url}")
            
            productos, precios, priceUnds, priceProm, elements = scrape_page(driver, url, wait)

            if not productos:
                intentos_fallidos += 1
                print(f"Intento fallido {intentos_fallidos} de {MAX_INTENTOS_FALLIDOS}")
                if intentos_fallidos >= MAX_INTENTOS_FALLIDOS:
                    print("Alcanzado el máximo de intentos fallidos. Finalizando...")
                    break  # Cambiado de productos_encontrados = False a break
                continue

            # Verificar que todas las listas tengan la misma longitud
            min_length = min(len(productos), len(precios), len(priceUnds), len(priceProm), len(elements))
            if min_length > 0:
                all_productos.extend(productos[:min_length])
                all_precios.extend(precios[:min_length])
                all_priceUnd.extend(priceUnds[:min_length])
                all_priceProm.extend(priceProm[:min_length])
                all_elements.extend(elements[:min_length])
                
                print(f"Productos encontrados en esta página: {min_length}")
                print(f"Total de productos acumulados: {len(all_productos)}")
                
            intentos_fallidos = 0
            page += 1
            time.sleep(2)

        # Verificar si se recolectaron productos
        if len(all_productos) > 0:
            try:
                print(f"Creando DataFrame con {len(all_productos)} productos...")
                
                # Asegurar que todas las listas tengan la misma longitud
                min_length = min(len(all_productos), len(all_precios), len(all_priceUnd), 
                               len(all_priceProm), len(all_elements))
                
                df = pd.DataFrame({
                    'Producto': all_productos[:min_length],
                    'Precio': all_precios[:min_length],
                    'Precio_Unidad': all_priceUnd[:min_length],
                    'Precio_Promocion': all_priceProm[:min_length],
                    'Link': all_elements[:min_length]
                })

                # Aplicar las funciones a la columna 'Producto'
                df['Peso'] = df['Producto'].apply(extraer_peso)
                df['Categoria'] = df['Producto'].apply(extraer_categoria)
                df['Marca'] = df['Producto'].apply(extraer_marca)
                df['Fecha'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                df['link'] = df['Link'].apply(limpiar_link)

                print(f"\nTotal de productos encontrados: {len(df)}")
                return df
            except Exception as e:
                print(f"Error al crear el DataFrame: {e}")
                return None
        else:
            print("No se encontraron productos.")
            return None

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

# Función para limpiar los datos de la columna 'Link'
def limpiar_link(link):
    link_str = str(link)
    return link.replace("{'href': '", "").replace("'}", "")


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
    df_productos = scrape_exito_products()
    guardar_resultados(df_productos)
    print("Proceso de scraping finalizado")
