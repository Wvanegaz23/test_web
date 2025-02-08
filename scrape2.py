import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
from tqdm import tqdm
import json
from datetime import datetime
import os

class ProductScraper:
    def __init__(self, checkpoint_file='scraping_checkpoint.json'):
        self.checkpoint_file = checkpoint_file
        self.processed_urls = self._load_checkpoint()
        
    def _load_checkpoint(self):
        """Carga el checkpoint si existe"""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_checkpoint(self, url, data):
        """Guarda el progreso en el checkpoint"""
        self.processed_urls[url] = data
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.processed_urls, f)
    
    def _initialize_driver(self):
        """Inicializa el driver de Chrome con la configuración adecuada"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        return webdriver.Chrome(options=chrome_options)
    
    def extraer_informacion(self, driver, url_producto):
        """Extrae PLU y puntos de un producto"""
        try:
            driver.get(url_producto)
            time.sleep(3)  # Esperar a que cargue la página
            
            # Extraer PLU
            plu = "N/A"
            try:
                plu_element = driver.find_element(By.CLASS_NAME, 'product-title_product-title__specification__UTjNc')
                if plu_element:
                    texto_completo = plu_element.text
                    plu = texto_completo.split('PLU: ')[-1] if 'PLU:' in texto_completo else "N/A"
            except Exception as e:
                print(f"Error al extraer PLU: {str(e)}")
            
            # Extraer Puntos
            puntos = "N/A"
            try:
                puntos_element = driver.find_element(By.CSS_SELECTOR, '[data-fs-product-details-puntos__qty="true"]')
                if puntos_element:
                    puntos = puntos_element.text
            except Exception as e:
                print(f"Error al extraer puntos: {str(e)}")
                
            # Extraer EAN
            ean = "N/A"
            try:
                # Buscar el script que contiene el atributo 'data-flix-ean'
                ean_element = driver.find_element(By.CSS_SELECTOR, 'script[data-flix-ean]')
                
                # Extraer el valor del atributo 'data-flix-ean'
                if ean_element:
                    ean = ean_element.get_attribute('data-flix-ean')
            except Exception as e:
                print(f"Error al extraer ean: {str(e)}")
                
            return {"PLU": plu, "Puntos": puntos,"EAN": ean}
                
        except Exception as e:
            print(f"Error general en la extracción: {str(e)}")
            return {"PLU": "N/A", "Puntos": "N/A", "EAN":"N/A"}

    def procesar_lote(self, urls, max_retries=3):
        """Procesa un lote de URLs con reintentos"""
        driver = self._initialize_driver()
        resultados = {}
        
        try:
            for url in urls:
                for intento in range(max_retries):
                    try:
                        resultado = self.extraer_informacion(driver, url)
                        resultados[url] = resultado
                        break
                    except WebDriverException:
                        if intento == max_retries - 1:
                            print(f"Fallo después de {max_retries} intentos para URL: {url}")
                            resultados[url] = {"PLU": "ERROR", "Puntos": "ERROR"}
                        else:
                            print(f"Reintentando URL: {url}")
                            time.sleep(5)  # Esperar antes de reintentar
                            driver.quit()
                            driver = self._initialize_driver()
                
                time.sleep(2)  # Esperar entre solicitudes
        finally:
            driver.quit()
            
        return resultados

    def procesar_urls_en_lotes(self, df, tamano_lote=50):
        """Procesa todas las URLs en lotes"""
        urls_pendientes = [url for url in df['link'] if url not in self.processed_urls]
        total_urls = len(urls_pendientes)
        
        print(f"Total de URLs pendientes: {total_urls}")
        
        for i in tqdm(range(0, total_urls, tamano_lote)):
            lote_urls = urls_pendientes[i:i + tamano_lote]
            print(f"\nProcesando lote {i//tamano_lote + 1}")
            
            resultados_lote = self.procesar_lote(lote_urls)
            
            # Guardar resultados del lote
            for url, resultado in resultados_lote.items():
                self._save_checkpoint(url, resultado)
            
            # Guardar resultados parciales en CSV
            self._guardar_resultados_parciales(df)
            
            print(f"Lote completado. Guardado checkpoint.")
    
    def _guardar_resultados_parciales(self, df_original):
        """Guarda los resultados parciales en un CSV"""
        df_resultado = df_original.copy()
        
        # Agregar columnas de resultados
        df_resultado['PLU'] = df_resultado['link'].map(
            lambda x: self.processed_urls.get(x, {}).get('PLU', 'Pendiente'))
        df_resultado['Puntos'] = df_resultado['link'].map(
            lambda x: self.processed_urls.get(x, {}).get('Puntos', 'Pendiente'))
        df_resultado['EAN'] = df_resultado['link'].map(
            lambda x: self.processed_urls.get(x, {}).get('EAN', 'Pendiente'))
        

        # Guardar con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        df_resultado.to_csv(f'resultados_parciales_{timestamp}.csv', index=False)

# Uso del scraper
def procesar_archivo_grande(archivo_csv):
    # Cargar el DataFrame
    df = pd.read_csv(archivo_csv)
    
    # Inicializar y ejecutar el scraper
    scraper = ProductScraper()
    scraper.procesar_urls_en_lotes(df)
    
    # Guardar resultados finales
    df_final = df.copy()
    df_final['PLU'] = df_final['link'].map(lambda x: scraper.processed_urls.get(x, {}).get('PLU', 'N/A'))
    df_final['Puntos'] = df_final['link'].map(lambda x: scraper.processed_urls.get(x, {}).get('Puntos', 'N/A'))
    df_final['EAN'] = df_final['link'].map(lambda x: scraper.processed_urls.get(x, {}).get('EAN', 'N/A'))    
    
    df_final.to_csv('resultados_finales.csv', index=False)
    print("\nProceso completado. Resultados guardados en 'resultados_finales.csv'")

# Ejemplo de uso
procesar_archivo_grande('productos_exito.csv')