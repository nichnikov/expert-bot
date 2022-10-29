from src.config import text_storage


text_storage.delete_all_from_table("answers")
text_storage.delete_all_from_table("etalons")
text_storage.delete_all_from_table("stopwords")
