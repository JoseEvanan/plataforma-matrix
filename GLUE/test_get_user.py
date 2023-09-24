
import boto3
sts = boto3.client('sts')
client = boto3.client('iam')
users = client.list_users()
#pprint(users)

for k,v in os.environ.items():
    print (f'{k}={v}')
    
raise
print()
#_get_default_session().client(*args, **kwargs)
t = boto3._get_default_session()
print(vars(t))    
print(vars(t._session))
print(vars(t._loader))

raise
print(sts.get_caller_identity())

pprint(vars(context))
#{"AttributeKey": "EventName", "AttributeValue": "RunInstances"}
client = boto3.client("cloudtrail")
response = client.lookup_events(
    LookupAttributes=[
        {
            'AttributeKey': 'EventName',
            'AttributeValue': 'CreateLogStream'
        },
    ],
    StartTime=datetime(2023, 9, 23),
    EndTime=datetime(2023, 9, 25)
)
pprint(response)