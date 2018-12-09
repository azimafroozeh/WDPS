#from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
import sys
import unicodedata
  
text = "This is a sample sentence, showing off the stop words filtration."

tbl = dict.fromkeys(i for i in range(sys.maxunicode) if unicodedata.category(chr(i)).startswith('P'))
text_no_pontuation = text.translate(tbl)
  
#stop_words = set(stopwords.words('english')) 

stop_words = {'its', 'against', 'what', 'so', 'll', "it's", 'where', "wasn't", 'about', 'didn', 'a', 'no', 'we', 'y', 'couldn', "haven't", 'is', 'them', 'be', 'should', 'mustn', "mightn't", 'between', "doesn't", 'her', 'who', 'why', 'd', 've', 'as', 'myself', "didn't", 'having', 'shan', "won't", 'until', 'and', 'itself', 'but', 'had', 'by', 'before', 'each', 'not', 'hadn', "shan't", 'above', 'yours', 'yourself', 'they', 'other', 'don', 'then', "you'd", 'down', 'your', 'such', 'weren', 'ours', 't', 'wouldn', 'does', 'needn', 'the', 'on', "isn't", 'our', 'after', 'most', 'themselves', 'through', 'again', 'there', 'both', 'in', 'shouldn', 'with', 'being', 'those', "you'll", 'ain', 'than', 'he', 'do', 'hasn', 'me', 'if', 'nor', 'aren', 'wasn', 'all', 'can', 'ma', 'you', "wouldn't", 're', 'because', "hasn't", "should've", 'my', 'will', 'from', 'under', 'own', 'to', 'him', 'did', 'here', 'same', 'when', 's', 'or', 'doing', 'o', "mustn't", 'she', "hadn't", 'few', "you're", 'these', "you've", 'while', 'am', 'i', 'too', "aren't", 'up', 'herself', 'now', 'that', 'are', 'very', 'at', 'only', 'm', 'out', "weren't", 'off', 'during', 'his', 'further', 'just', 'of', 'has', 'doesn', 'theirs', 'which', 'an', 'whom', 'more', 'isn', 'mightn', "couldn't", 'for', 'was', 'below', 'any', 'their', 'won', "needn't", 'yourselves', 'over', 'some', 'haven', 'been', 'once', 'into', 'how', 'this', 'himself', 'have', "don't", "shouldn't", 'hers', 'it', "that'll", 'ourselves', "she's", 'were'}

word_tokens = word_tokenize(text_no_pontuation) 

# Remove default stop words
filtered_sentence = [w for w in word_tokens if not w in stop_words] 
print('TEXT: ', text)
print('TEXT WITHOUT PONCTUATION ', text_no_pontuation)
print()
print('TOKENS: ',word_tokens) 
print('TOKENS WITHOUT STOPWORDS: ',filtered_sentence) 
print()
print('STOPWORDS', stop_words)