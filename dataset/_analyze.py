import pandas as pd
inv = pd.read_csv('invocations_per_function_md.anon.d01.csv')
dur = pd.read_csv('function_durations_percentiles.anon.d01.csv')
mem = pd.read_csv('app_memory_percentiles.anon.d01.csv')

print('=== invocations ===')
print('shape:', inv.shape)
print('triggers:', inv['Trigger'].value_counts().to_dict())
total_per_func = inv.iloc[:, 4:].sum(axis=1)
print('total_invocations stats:')
print(total_per_func.describe())

# high-frequency functions (could be "frequency" tasks)
high_freq = inv[total_per_func > 100]
print(f'\nFunctions with >100 invocations/day: {len(high_freq)}')
print('  triggers:', high_freq['Trigger'].value_counts().to_dict())

# periodic: functions that appear in many minutes
nonzero_mins = (inv.iloc[:, 4:] > 0).sum(axis=1)
periodic = inv[nonzero_mins > 100]
print(f'Functions active in >100 minutes: {len(periodic)}')

print('\n=== durations ===')
print('shape:', dur.shape)
print(dur[['Average','Count','Minimum','Maximum']].describe())

# join invocations with durations to classify
merged = inv[['HashOwner','HashApp','HashFunction','Trigger']].copy()
merged['total_invocations'] = total_per_func.values
merged['active_minutes'] = nonzero_mins.values
merged = merged.merge(dur[['HashOwner','HashApp','HashFunction','Average','Count','Maximum','percentile_Average_50','percentile_Average_99']], 
                       on=['HashOwner','HashApp','HashFunction'], how='inner')
print(f'\nMerged (inv+dur): {len(merged)} rows')
print(merged.describe())

# Candidate frequency: timer triggers OR high invocation rate with regular pattern
freq_candidates = merged[(merged['Trigger'] == 'timer') | (merged['active_minutes'] > 500)]
print(f'\nFrequency candidates (timer or active>500min): {len(freq_candidates)}')
print(freq_candidates[['total_invocations','active_minutes','Average']].describe())

lat_candidates = merged[~merged.index.isin(freq_candidates.index)]
print(f'Latency candidates: {len(lat_candidates)}')
print(lat_candidates[['total_invocations','active_minutes','Average']].describe())

print('\n=== memory ===')
print('shape:', mem.shape)
print(mem[['SampleCount','AverageAllocatedMb']].describe())
