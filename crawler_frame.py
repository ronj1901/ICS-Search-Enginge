import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager, Link
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from collections  import defaultdict
from bs4 import BeautifulSoup
import urllib
from lxml import html
import requests
from urlparse import urlparse, urljoin
import operator
#import tldextract

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set()
    if not os.path.exists("successful_urls.txt") else
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

subdomains_visited = defaultdict(int)  # keeping  track of subdomain for analytics
dictOfOutgoinglinks = defaultdict(int)  # keeping track of outgoing links for analytics
numOfInvalidLinks = 0
@Producer(ProducedLink, Link)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "10677794_66729498_62095977"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR S17 UnderGrad 10677794, 66729498, 62095977"

        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get_new(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        # i am thinking of writing to a  file
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas

#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(rawDatas):
    outputLinks = list()
    global numOfInvalidLinks
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    for eachRaw in rawDatas:
        soup = BeautifulSoup(eachRaw.content, 'html.parser')
        url = eachRaw.url
        parsed_url  = urlparse(url)
        baseUrl  = "http://" + parsed_url.netloc

        all_links =  soup.find_all('a')

        if eachRaw.is_redirected:
            url = eachRaw.final_url

        if eachRaw.http_code  >= 400:  #bad url found
            eachRaw.bad_url = True
            numOfInvalidLinks +=1


        if ".ics.uci.edu" in parsed_url.netloc.lower() and parsed_url.netloc != "www.ics.uci.edu":
                subdomains_visited[parsed_url.netloc] += 1
        dictOfOutgoinglinks[url] = len(all_links)


        for link in all_links: #mostly getting relative urls  here
            innerLink = link.get("href")

            if  innerLink == None or innerLink == "#" or innerLink.startswith("mailto") or innerLink.find("javascript") == 0:
                continue

            if innerLink.startswith('/'): # we know that this is a reltative url
                innerLink = urljoin(baseUrl, innerLink)
                outputLinks.append(innerLink)
            outputLinks.append(innerLink)

    f = open("Analytics.txt", "w")
    f.write("==========================ANALYTICS PART================================\n\n\n")
    f.write("Subdomains visited and their count:\n")

    for key in subdomains_visited:
        f.write(key + " : " + str(subdomains_visited[key]) + "  \n")

    f.write("\n\n")

    f.write("outgoing links and their count:\n\n")
    f.write("=========================================================================\n\n\n")
    for k, v in sorted(dictOfOutgoinglinks.iteritems(), key = lambda kv: kv[1], reverse= True):
        f.write(k + " --> " + str(v)  + "\n")

    f.write("\n\n")

    if len(dictOfOutgoinglinks) > 0:
        maximum_key = max(dictOfOutgoinglinks.iteritems(), key=operator.itemgetter(1))[0]
        f.write("The url with the maximum outgoung link is   " + maximum_key + "with " + str(dictOfOutgoinglinks[maximum_key]))

    f.write("\n\n")
    f.write(" The total number of invalid links:     " + str(numOfInvalidLinks) )
    f.write("\n\n")
    f.close()

    print(numOfInvalidLinks)
    return outputLinks


def maxRep(d):
    for key,value in d.items():
        if value > 3:
            return False
        return True

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    global numOfInvalidLinks


    try:
        return_value = True
        parsed = urlparse(url)

        stringvar = url.split("/")
        fq = defaultdict(int)
        for w in stringvar:
            fq[w] += 1

        if maxRep(fq) == False:
            return False

        if parsed.scheme not in set(["http", "https"]):
            return_value =  False

        if "cbcl.ics.uci.edu/doku.php" in url:
            return False
        if "duttgroup.ics.uci.edu" in url:
            return False
        if "ics.uci.edu/ugrad/policies/Add_Drop_ChangeOption.php" in url:
            return False
        if "archive.ics.uci.edu" in url:
            return False
        if "calender.ics.uci.edu" in url:
            return False
        if "cbcl.ics.uci.edu/doku.php" in url:
            return False
        if parsed.hostname == None:
            return_value = False
        else:
            return_value = ".ics.uci.edu" in parsed.hostname \
                and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
                + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                + "|thmx|mso|arff|rtf|jar|csv"\
                + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        # if not return_value:
        #     numOfInvalidLinks += 1

#        print return_value
        return return_value

    except TypeError:
        print ("TypeError for ", parsed)
