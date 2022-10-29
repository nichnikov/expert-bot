def scrub(table_name):
    return ''.join(chr for chr in table_name if chr.isalnum())

r = scrub("queries")
print(r)

table = "queries"
query = 'SELECT * FROM {}'.format(table)
print(query)