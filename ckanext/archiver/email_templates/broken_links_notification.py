# coding: utf-8

from pylons import config

"""
    A template file for resource broken link notification emails.
"""


def message(itemList):
    items = []
    for item in itemList:
        items.append(
            singleItem.format(
                package_id=item["package_id"],
                package_title=item["package_title"],
                resource_id=item["resource_id"],
                broken_url=item["broken_url"],
                site_url=config['ckan.site_url'],
            )
        )

    separator = '\n'
    return messageTemplate.format(amount=len(itemList), items=separator.join(items))


subject = u"{amount} broken link(s) in your dataset(s) in Avoindata.fi - Yksi tai useampi rikkinäinen linkki tietoaineistoissasi Avoindata.fi-palvelussa" # noqa


messageTemplate = u"""
Hei,
 
Ylläpidät tietoaineistoja Avoindata.fi-palvelussa. Aineistoissasi on tällä hetkellä (1) tai useampi rikkinäinen linkki,
jotka on listattu alla. Voit päivittää linkit kirjautumalla palveluun, valitsemalla korjattavan datasetin ja päivittämällä linkin.
Jos sinulla on kysyttävää, opastamme sinua tarpeen vaatiessa osoitteessa avoindata@vrk.fi.
 
Ystävällisin terveisin,
Avoindata.fi tuki
___

Hello,
 
You have uploaded a dataset or datasets in Avoindata.fi. You have {amount} broken link(s) in your datasets.
You can update the link(s) by logging in, navigating to the broken resource.
 
Should you have any questions or need help, please get in touch with us at avoindata@vrk.fi.
 
Best regards,
Avoindata.fi support

___


{items}

""" # noqa


singleItem = u"""Tietoaineisto - Dataset:
{package_title} ( {site_url}/data/fi/dataset/{package_id} )

Resurssi - Resource:
{site_url}/data/fi/dataset/{package_id}/resource/{resource_id}

Rikkinäinen linkki - Broken link:
{broken_url}
___
"""
