import concurrent.futures
import multiprocessing
import os
import tempfile
import time
from urllib import request

import face_recognition
import pymongo

client = pymongo.MongoClient(os.environ.get("MONGO_URL"))


def count_documents_that_have_has_face_field():
    db_1 = client["zymio"]
    return db_1.products.count_documents({"has_face": {"$exists": True}})


def has_face(*, image_url: str) -> bool:
    with tempfile.NamedTemporaryFile() as image_file:
        request.urlretrieve(image_url, image_file.name)
        image_file.seek(0)
        image = face_recognition.load_image_file(image_file.name)
        return len(face_recognition.face_locations(image)) > 0


def update_has_face(product_id: str):
    if not product_id:
        print("product_id if false", product_id)
        return None
    try:
        db_1 = client["zymio"]
        if missing_field_document := db_1.products.find_one({"_id": product_id}):
            image_url = missing_field_document["productImages"][0]["source"]
            product_has_face = has_face(image_url=image_url)
            db_1.products.update_one(
                {"_id": missing_field_document["_id"]},
                {"$set": {"has_face": has_face(image_url=image_url)}}
            )
            print(
                f"""{missing_field_document["productName"]}: {image_url} {"has" if product_has_face else "doesn't have"} face""")
    except Exception as e:
        print("--------------------")
        print("Exception while updating has_face field: ", e)
        print("--------------------")


if __name__ == "__main__":
    db = client["zymio"]
    while db.products.find_one({"has_face": {"$exists": False}}):
        try:
            with concurrent.futures.ProcessPoolExecutor(multiprocessing.cpu_count()) as executor:
                start_time = time.perf_counter()
                product_ids = [product.get("_id") for product in db.products.find(
                    {"has_face": {"$exists": False}}
                ).limit(100) if product.get("_id")]
                print("Product Id len: ", len(product_ids))
                result = list(executor.map(update_has_face, product_ids))
                finish_time = time.perf_counter()
            print(f"Program finished in {finish_time - start_time} seconds")
        except Exception as e:
            print("--------------------")
            print("Exception while querying missing has_face field: ", e)
            print("--------------------")
