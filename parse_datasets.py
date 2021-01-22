import json


def main():
    stations = []
    fields = {'fields': {}}
    with open('datasets.json', 'r') as f:
        doc = json.load(f)
        for dataset in doc.get('datasets'):
            metas = dataset.get('metas')
            # stations.append({
            #     'slug': dataset.get('datasetid'),
            #     'keyword': metas.get('keyword'),
            #     'title': metas.get('title'),
            #     'description': metas.get('description')
            # })
            # print(dataset.get('datasetid'))
            for field in dataset.get('fields'):
                if field:
                    if field.get("label") not in fields['fields']:
                        fields['fields'][field.get("label")] = {
                            'desc': set(),
                            'datasets': set()
                        }
                    if field.get("description", None):
                        fields['fields'][field.get("label")]['desc'].add(field.get("description"))
                    fields['fields'][field.get("label")]['datasets'].add(dataset.get('datasetid'))
                # print(f'\t{field.get("label")}\t\t{field.get("description")}')

    print(fields)

    # with open('stations_fields.jsonl', 'w') as f:
    #     for s in stations:
    #         f.write(json.dumps(s)+'\n')



if __name__ == '__main__':
    main()