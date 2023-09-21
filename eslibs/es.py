from elasticsearch import Elasticsearch, exceptions

def New(server):
    #extra = json.loads(server.extra)
    if server.login != "":
        es = Elasticsearch(
            hosts=[{"host": server.host, "port": server.port, "scheme": server.schema}],
            ssl_show_warn=False,
            use_ssl = False,
            verify_certs = False,
            #ssl_assert_fingerprint=extra['fingerprint'],
            http_auth=(server.login, server.password)
        )
    else:
        es = Elasticsearch(
            hosts=[{"host": server.host, "port": server.port, "scheme": server.schema}],
            ssl_show_warn=False,
            use_ssl = False,
            verify_certs = False
        )
    
    return es

def save_doc(es, index, post): 
    try:
        result = es.index(index=index, body=post)
        print("Save Doc", result)
        return True
    except exceptions.ConflictError:
        return False