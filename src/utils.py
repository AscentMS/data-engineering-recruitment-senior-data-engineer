# src/utils.py
def standardize_postcode(postcode):
    if postcode:
        return postcode.replace(" ", "").upper()
    return None