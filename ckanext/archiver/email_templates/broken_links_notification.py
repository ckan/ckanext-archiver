from pylons import config

"""
    A template file for resource broken link notification emails.
"""


def message(itemList):
    items = []
    for item in itemList:
        item["broken_url"] = (item["broken_url"].encode('ascii', 'ignore')).decode("utf-8")
        item["package_title"] = (item["package_title"].encode('ascii', 'ignore')).decode("utf-8")
        items.append(
            singleItem.format(
                package_id=item["package_id"],
                package_title=item["package_title"],
                resource_id=item["resource_id"],
                broken_url=item["broken_url"],
                site_url=config['ckan.site_url'],
            ).encode('utf-8')
        )

    separator = '\n'
    return messageTemplate.format(amount=len(itemList), items=separator.join(items))


subject = "{amount} broken link(s) in your datasets"


messageTemplate = """
You have {amount} broken link(s) in your datasets.
You can update the link(s) by logging in and navigating to the broken resource.

{items}
---

Best regards

Avoindata.fi support
avoindata@vrk.fi
"""


singleItem = """---

Dataset:
{package_title} ( {site_url}/data/fi/dataset/{package_id} )

Resource:
{site_url}/data/fi/dataset/{package_id}/resource/{resource_id}

Broken link:
{broken_url}
"""
