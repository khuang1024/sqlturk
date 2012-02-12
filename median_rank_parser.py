def parse_into_list(string):
    '''This function turns the string into list. The delimiter is |.'''
    num_list = [] 
    numbers = re.split("\|", string)
    for num in numbers:
        num_list.append(num.strip())
    return num_list

def extract_into_list(hit_id, qnum):
    '''This function parses the answers by given the HIT ID and the question
    number. Then, it returns a 2-dimensional list containing all these
    answers.'''
    if type(qnum) == str:
        qno = qnum
    else:
        qno = str(qnum)
    ranks = []
    f = open ("ranking_answer_log.csv")
    lines = f.read().split("\n")
    for line in lines:
        if re.match(hit_id, line): # match is better than search here
            fields = re.split("&", line)
            for field in fields:
                if re.search("q"+qno+"\[\]=", field): # field = p1[]=1|2|3..
                    ranks.append(parse_into_list(re.split("=", field)[1]))
    return ranks

def atom_median_rank(ranks):
    '''This function implements the median rank algorithm which is used for 
    sorting the results. The algorithm checks the positions of arrays from the 
    head (where the highest-rank item is) to the tail (where the lowes-rank 
    item is) simultaneously on each list. After checking each position, it sums
    up the presence of each element. If the presence of an element reaches the 
    half size of the list, this element is added into a new list, which is the 
    final result of sorting. However, there are some other consideration. Maybe
    two elements have the same presences ("conflict") after this round, however
    , they may be supposed to have different order. For instance, say element 
    #1 appears in position 1, 4, while element #2 appears 3, 4. Maybe after the
    4th round, they all gain equal counts, however, #1 appeared earilier than #2
    . Therefore #1 should be placed before #2.'''
    result = [] # the final result
    keys = [] # the keys in our ranking
    record = {} # the record of how many times an element appears so far
    addition = {} # the record of additional info for sovlving"conflict"
    
    for i in ranks[0]:
        keys.append(i)

    for i in keys: 
        record.setdefault(i, 0)
        addition.setdefault(i,0)
    # record = dict.fromkeys(["1","2","3","4","5","6","7","8","9","10"], 0)

    for i in range(len(keys)):
        temp = {} 

        for rank in ranks:
            index = rank[i]
            # index = str(rank.index(str(i+1))+1)
            record[index] += 1
            addition[index] += i # add up the position where it appears

        for j in keys:
            if record[j] >= len(ranks)/2 + 0.1 and j not in result:
                temp[j] = record[j]

        for v in sorted(temp.values()):
            if list(temp.values()).count(v) == 1:
                for k in temp.keys():
                    if temp[k] == v and k not in result:
                        result.append(k)
            else:
                again = {}
                for k in temp.keys():
                    if temp[k] == v and k not in result:
                        #print(str(k) + "=" + str(addition[k]))
                        again[addition[k]] = k
                for k in sorted(again.keys()):
                    result.append(again[k])
        
    return result

def random_keys(n):
    '''This function randomly generate some keys between 1 and 10. The number
    of these random keys is indicated by n.'''
    if type(n) == int:
        num = n
    else:
        num = int(n)

    keys = [];
    while len(keys) < num:
        random_num = str(random.randint(1, 10))
        if random_num not in keys:
            keys.append(random_num)

    return keys

def filter_ranks(ranks, keys):
    '''This function accepts the ranks (the original 2-dimensional ranks) and
    a list of keys. It generates a new 2-dimensional list which only keeps the
    elements in keys. The new 2-dimensional list also keeps the original order
    of these elements.'''
    new_ranks = []
    for rank in ranks:
        new_rank = []
        for key in rank:
            if key in keys:
                new_rank.append(key)
        new_ranks.append(new_rank)
    return new_ranks
        

def get_all_hits():
    '''This function returns all the HIT IDs in the answer log file.'''
    hit_ids = []
    f = open ("ranking_answer_log.csv")
    lines = f.read().split("\n")
    for line in lines:
        hit_id = re.split("&", line)[0]
        if hit_id not in hit_ids and hit_id:
            hit_ids.append(hit_id)
    return hit_ids
                
def median_rank(hit_id, top):
    '''This function runs the median rank algorithm for a single HIT, including
    both the first and second questions of a HIT.'''
    ranks_p1 = filter_ranks(extract_into_list(hit_id, "1"), random_keys(top))
    ranks_p2 = filter_ranks(extract_into_list(hit_id, "2"), random_keys(top))
    #print(ranks_p1)
    #print(ranks_p2)
    print(hit_id + "\tq1\t" + str(atom_median_rank(ranks_p1)))
    print(hit_id + "\tq2\t" + str(atom_median_rank(ranks_p2)))
    print("------------------------------------------------------------------")

if __name__ == "__main__":
    import sys
    import re
    import random
    hits = get_all_hits()
    for hit in hits:
        median_rank(hit, sys.argv[1])


    # test
    #rand = random_keys(3)
    #origin = extract_into_list("2J5JKM05BMJTOZKMPXKPDRA8V1CDI", 1)
    #ranks = filter_ranks(origin, rand)
    #print(ranks)
    #print(get_all_hits())
    #ranks = extract_into_list(sys.argv[1], "1")
    #result = atom_median_rank(ranks)
    #print (result)
