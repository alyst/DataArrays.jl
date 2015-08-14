# returns a tuple
# 1) permutation of x: y[result] = x sorts x in y
# 2) indices of each new group starts in y
# 3) group sizes
function groupsort_indexer(x::AbstractVector, ngroups::Integer, nalast::Bool=false)
    # translated from Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).

    # count group sizes, x[i] == 0 means NA
    n = length(x)
    counts = zeros(Int, ngroups + 1)
    for i = 1:n
        counts[x[i] + 1] += 1
    end

    # mark the start of each contiguous group of like-indexed data
    where = Array{Int}(ngroups + 1)
    if nalast
        # skip NA
        where[2] = 1
        for i = 3:ngroups+1
            where[i] = where[i-1] + counts[i-1]
        end
        # NA indexes
        where[1] = where[end] + counts[end]
    else
        where[1] = 1
        for i = 2:ngroups+1
            where[i] = where[i-1] + counts[i-1]
        end
    end

    # this is our indexer
    result = Array{Int}(n)
    for i in 1:n
        label = x[i] + 1
        result[where[label]] = i
        where[label] += 1
    end
    result, where, counts
end

groupsort_indexer(pv::PooledDataVector, nalast::Bool=false) = groupsort_indexer(pv.refs, length(pv.pool), nalast)
