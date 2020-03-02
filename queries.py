import aerospike
import sys
import logging
from aerospike import predicates as p
from aerospike import exception as ex


def connect_aerospike(config):
    try:
        return aerospike.client(config).connect()
    except ex.AerospikeError as e:
        logging.error("Connection error, host = {0}".format(config['hosts']))
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

def add_customer(customer_id, phone_number, lifetime_value):
    key = ("test", "cust_set", customer_id)
    bins = {"phone": phone_number, "ltv": lifetime_value}
    client.put(key, bins, meta={'ttl':60})

 
def get_ltv_by_id(customer_id):
    key = ("test", "cust_set", customer_id)
    (key, meta, bins) = client.get(key)
    print(client.get(key))
    if (bins == {}):
        logging.error("Requested non-existent customer " + str(customer_id))
    else:
        return bins.get("ltv")


def get_ltv_by_phone(phone_number):
    query = client.query("test", "cust_set")
    query.where(p.equals("phone", phone_number))
    query.select("ltv")
    print(query)
    for record in query.results():
        (key, meta, bins) = record
        return bins.get("ltv")
    logging.error('Requested phone number is not found ' + str(phone_number))

 
if __name__ == '__main__':

    config = {
        'hosts': [('127.0.0.1', 3000)]
    }


    client = connect_aerospike(config)

    client.index_string_create('test', 'phones', "phone_number", "ix_phone_number")


    for x in range(1000): 
        add_customer(x, "+7911191916757", x+100)
    print("Add customers.")

    queryResult=""
    for x in range(100): 
        queryResult+=str(get_ltv_by_id(x))+", "
    print("Search by key." )
    print(queryResult[0:100]+"...")

    queryResult=""
    for x in range(100):
        phone_number = "+7911191916757"
        queryResult+=phone_number+" -> "+str(get_ltv_by_phone_query(phone_number))+", "
    print("Query by phone number(with index).")
    print(queryResult[0:100]+"...")
