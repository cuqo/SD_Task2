import pywren_ibm_cloud as pywren
import json
import time
import io
import numpy
import datetime

N_SLAVES = 100
BUCKET_NAME = 'cuc-bucket'
WAIT = 0.5

def master(id, x, ibm_cos):    
    
    result = []
    memfile = io.BytesIO()
    numpy.save(memfile, result)
    memfile.seek(0)
    serialized = json.dumps(memfile.read().decode('latin-1'))

    ibm_cos.put_object(Bucket=BUCKET_NAME, Key='result.txt', Body=serialized)

    write_permission_list = []
    
    # 1. monitor COS bucket each X seconds
    if(N_SLAVES < 50):
        wait = N_SLAVES/10+1
    else:
        wait = N_SLAVES/10+5
    time.sleep(wait)

    # 2. List all "p_write_{id}" files
    listP = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix='p_write_')['Contents']    

    # 3. Order objects by time of creation
    listP = sorted(listP, key=lambda k: k['LastModified'])

    i = 0
    while(i < N_SLAVES):
        # 4. Pop first object of the list "p_write_{id}"
        first = listP.pop(0)['Key']

        # 5. Write empty "write_{id}" object into COS   
        id = first[8:]
        key = 'write_' +str(id)

        lastMod = ibm_cos.head_object(Bucket=BUCKET_NAME, Key='result.txt')['LastModified']       
        ibm_cos.put_object(Bucket=BUCKET_NAME, Key=key, Body="")

        # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=first)
        write_permission_list.append(str(id))

        # 7. Monitor "result.json" object each X seconds until it is updated
        modif = False
        while (modif == False):            
            lastMod2 = ibm_cos.head_object(Bucket=BUCKET_NAME, Key='result.txt')['LastModified'] 

            if(lastMod2 != lastMod):
                modif = True
            
            time.sleep(WAIT)
        
        # 8. Delete from COS “write_{id}”
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=key)
        i = i+1

        # 8. Back to step 1 until no "p_write_{id}" objects in the bucket
        time.sleep(WAIT)

    return write_permission_list


def slave(id, x, ibm_cos):
    found = False

    # 1. Write empty "p_write_{id}" object into COS
    petition ='p_write_' +str(id)
    key = 'write_' +str(id)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=petition, Body="")
    
    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"    
    while(not found):
        time.sleep(WAIT)

        try:
            ibm_cos.head_object(Bucket=BUCKET_NAME, Key=key)
            found = True
        except:
            pass

    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt        
    if (found):
        serialized = ibm_cos.get_object(Bucket=BUCKET_NAME, Key='result.txt')['Body'].read()
        memfile = io.BytesIO()
        memfile.write(json.loads(serialized).encode('latin-1'))
        memfile.seek(0)
        result = numpy.load(memfile)

        result = result.tolist()
        result.append(str(id))

        memfile = io.BytesIO()
        numpy.save(memfile, result)
        memfile.seek(0)
        serialized = json.dumps(memfile.read().decode('latin-1'))

        ibm_cos.put_object(Bucket=BUCKET_NAME, Key='result.txt', Body=serialized)

    # 4. Finish
    return
    # No need to return anything


def deleteTrash(ibm_cos):
    listT = ibm_cos.list_objects(Bucket=BUCKET_NAME)['Contents']

    for item in listT:        
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=item['Key'])
    

if __name__ == '__main__':   
      

    pw = pywren.ibm_cf_executor()
    ibm_cos = pw.internal_storage.get_client()
    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()

    # Get result.txt
    
    serialized = ibm_cos.get_object(Bucket=BUCKET_NAME, Key='result.txt')['Body'].read()
    memfile = io.BytesIO()
    memfile.write(json.loads(serialized).encode('latin-1'))
    memfile.seek(0)
    result = numpy.load(memfile)
    result = result.tolist()    

    # check if content of result.txt == write_permission_list
    print("Result: ", result)    
    print("Llista: ", write_permission_list[0])

    if(result == write_permission_list[0]):
        print("Les llistes son iguals")
    else: 
        print("Les llistes son diferents")

    deleteTrash(ibm_cos)