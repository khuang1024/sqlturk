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
    ranks = []
    f = open ("ranking_answer_log.csv")
    lines = f.read().split("\n")
    for line in lines:
        if re.match(hit_id, line): # match is better than search here
            fields = re.split("&", line)
            for field in fields:
                if re.search("q"+qnum+"\[\]=", field): # field = p1[]=1|2|3..
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
    
    for i in range(len(ranks[0])):
        keys.append(str(i+1))

    for i in keys:  
        record.setdefault(i, 0)
        addition.setdefault(i,0)
    # record = dict.fromkeys(["1","2","3","4","5","6","7","8","9","10"], 0)

    for i in range(len(ranks[0])):
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
                
def median_rank(hit_id):
    '''This function runs the median rank algorithm for a single HIT, including
    both the first and second questions of a HIT.'''
    ranks_p1 = extract_into_list(hit_id, "1")
    ranks_p2 = extract_into_list(hit_id, "2")
    print(hit_id + "\tq1\t" + str(atom_median_rank(ranks_p1)))
    print(hit_id + "\tq2\t" + str(atom_median_rank(ranks_p2)))
    print("------------------------------------------------------------------")

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

if __name__ == "__main__":
    import sys
    import re
    hits = get_all_hits()
    for hit in hits:
        median_rank(hit)

    #print(get_all_hits())
    #ranks = extract_into_list(sys.argv[1], "1")
    #result = atom_median_rank(ranks)
    #print (result)
    #test = [["2","1","3","4"],["3","1","4","2"]]
    #print (atom_median_rank(test))
