import pandas as pd
from pymongo import MongoClient

# 1. Conectar a MongoDB
client = MongoClient("mongodb://localhost:27017/")#puerto donde corre comunmente mongo
db = client["consulta-01"]  #nombre de mi base de datos en mongo
collection = db["datos_excel"] #nombre de la colección donde se guardarán los datos

def cargar_excel_en_mongodb(ruta_archivo, anio):
    df = pd.read_excel(ruta_archivo) #leemos el archivo Excel
    df["anio"] = anio  #Agregamos el año para diferenciar
    data = df.to_dict(orient="records")#convertimos el DataFrame a una lista de diccionarios
    collection.insert_many(data)#insertamos los datos en la colección
    print(f"Insertados {len(data)} registros del archivo {ruta_archivo}")

# 3. Cargar los archivos
#cargar_excel_en_mongodb("data/2022.xlsx", 2022)
#cargar_excel_en_mongodb("data/2023.xlsx", 2023)

#consulta 1 cuántos partidos terminaron con un Retired por año
print("\n Partidos terminados como 'Retired' por año:")
for doc in collection.aggregate([{"$match": {"Comment": "Retired"}},{"$group": {"_id": "$anio", "retiros": {"$sum": 1}}}]):
    print(f"Año {doc['_id']}: {doc['retiros']} retiros")


#consulta 2 top 5 ganadores con más victorias en 2023
print("\n Top 5 ganadores con más partidos ganados en 2023:")
for doc in collection.aggregate([{"$match": {"anio": 2023}},{"$group": {"_id": "$Winner", "victorias": {"$sum": 1}}},{"$sort": {"victorias": -1}},{"$limit": 5}]):
    print(f"{doc['_id']}: {doc['victorias']} victorias")
