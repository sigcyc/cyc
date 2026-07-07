# Proposal: instrument_id support

1. We add one more field in `df_types.yaml` instrument_id with default of None
2. In the enrich place. Follow this rule
if one of sym or instrument_id exist, use cyc_ref_data sym_to_id / id_to_sym to create the other
