def aggregate(aggregationMethod, knownValues):
    if aggregationMethod == 'average':
        return float(sum(knownValues)) / float(len(knownValues))
    elif aggregationMethod == 'sum':
        return float(sum(knownValues))
    elif aggregationMethod == 'last':
        return knownValues[len(knownValues) - 1]
    elif aggregationMethod == 'max':
        return max(knownValues)
    elif aggregationMethod == 'min':
        return min(knownValues)
    else:
        raise Exception("Unrecognized aggregation method %s" %
                        aggregationMethod)