import requests
import re
class PanQuery:
    """PANGAEA Query Class
        This class allows to query PANGAEA and to access query results

        Parameters
        ----------
        query : str
            The query string following the specs at www.pangaea.de
        bbox : set
            The bounding box to define  geographical search constraints following the GeoJSON specs
            example : bbox=(minlon, minlat,  maxlon, maxlat)
        limit : int
            The expected number of search results (max =100)
        offset : int
            The offset of the search to continue results retrieval

        Attributes
        ----------
        totalcount : int
            The number of total search results
        error : str
            In case an error occurs this attribute holds the latest one
        query : str
            The query (see above)
        result : list
            A list of retrieved search results

        """
    PANGAEA_QUERY_URL = 'https://www.pangaea.de/advanced/search.php?q='
    def __init__(self, query, bbox =(), limit=10, offset=0):
        self.totalcount=0
        self.error = None
        self.query = query
        self.result = self._search(self.query, bbox, limit, offset)
        if self.error is not None:
            print('ERROR: '+self.error)


    def _search(self,query, bbox =(), limit=10, offset=0):
        """
                Performs the search request and stores the results in a list
                this method is called by the constructur

        """
        response = []
        panquery = self.PANGAEA_QUERY_URL+query
        try:
            if len(bbox) ==4:
                minlon, minlat,  maxlon, maxlat, = bbox
                panquery += '&maxlat='+str(maxlat)+'&minlon='+str(minlon)+'&maxlon='+str(maxlon)+'&minlat='+str(minlat)
            if offset != 0:
                panquery +='&offset='+str(offset)
            if limit !=10:
                panquery+='&count='+str(limit)

            r = requests.get(panquery)
            res = r.json()
            if r.status_code == 200:
                self.totalcount= res.get('totalCount')
                response = res.get('results')
                position = offset
                for rset in response:
                    if re.search(r'>[0-9]+\sdatasets<', rset.get('html')):
                        rset['type']='collection'
                    else:
                        rset['type']='member'
                    rset['position'] = position
                    position+=1
            elif r.status_code == 500:
                self.error = res.get('error')
            else:
                self.error = 'Request failed: response code: '+str(r.status_code)
        except Exception as e:
            self.error = 'Request failed: '+str(e)
        return response
