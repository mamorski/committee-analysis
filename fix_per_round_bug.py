#!/usr/bin/env python3
import json
import sys

def main():
    notebook_path = '/Users/igor-mamorsky/private-repos/committee-analysis/simulation_report.ipynb'
    
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
        
        print(f"Loaded notebook with {len(notebook['cells'])} cells")
        
        # Find the cell containing process_simulation_for_pdf
        target_cell_idx = None
        for idx, cell in enumerate(notebook['cells']):
            if cell['cell_type'] == 'code':
                source = ''.join(cell['source'])
                if 'def process_simulation_for_pdf' in source:
                    target_cell_idx = idx
                    print(f"Found target function in cell {idx}")
                    break
        
        if target_cell_idx is None:
            print("ERROR: Could not find the target cell")
            sys.exit(1)
        
        # Get the cell
        cell = notebook['cells'][target_cell_idx]
        
        # Convert source to string
        original_source = ''.join(cell['source'])
        print(f"Original source length: {len(original_source)}")
        
        # Fix the per-round statistics bug
        buggy_code = '''    if sim_message_counts:
        per_protocol_dfs = build_per_round_statistics_by_protocol(sim_message_counts)
        if not df_per_round.empty:
            ax_per_round.text(0.05, 0.95, "Per-Round Message Statistics:",
                             transform=ax_per_round.transAxes, fontsize=10, fontweight='bold',
                             verticalalignment='top')
            
            per_round_str = tabulate(df_per_round, headers='keys', tablefmt='simple', showindex=False, floatfmt='.1f')
            ax_per_round.text(0.05, 0.85, per_round_str, transform=ax_per_round.transAxes,
                             fontsize=7, verticalalignment='top', fontfamily='monospace')'''
        
        fixed_code = '''    if sim_message_counts:
        per_protocol_dfs = build_per_round_statistics_by_protocol(sim_message_counts)
        if per_protocol_dfs:
            y_pos = 0.95
            for protocol, df in per_protocol_dfs.items():
                if not df.empty:
                    ax_per_round.text(0.05, y_pos, f"{protocol} - Per-Round Stats:",
                                     transform=ax_per_round.transAxes, fontsize=9, fontweight='bold',
                                     verticalalignment='top')
                    y_pos -= 0.05
                    
                    # Show first 10 rounds
                    per_round_str = tabulate(df.head(10), headers='keys', tablefmt='simple', showindex=False, floatfmt='.1f')
                    ax_per_round.text(0.05, y_pos, per_round_str, transform=ax_per_round.transAxes,
                                     fontsize=6, verticalalignment='top', fontfamily='monospace')
                    y_pos -= (0.15 + 0.01 * min(len(df), 10))
                    
                    if y_pos < 0.05:
                        break'''
        
        if buggy_code in original_source:
            modified_source = original_source.replace(buggy_code, fixed_code)
            print("Fixed the per-round statistics bug")
        else:
            print("Could not find the buggy code to replace")
            sys.exit(1)
        
        print(f"Modified source length: {len(modified_source)}")
        
        # Convert back to list of strings (one per line)
        source_lines = []
        for line in modified_source.split('\n'):
            source_lines.append(line + '\n')
        
        # Remove trailing newline from last line
        if source_lines and source_lines[-1] == '\n':
            source_lines.pop()
        elif source_lines:
            source_lines[-1] = source_lines[-1].rstrip('\n')
        
        # Update the cell
        notebook['cells'][target_cell_idx]['source'] = source_lines
        
        # Save the notebook
        with open(notebook_path, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, indent=1)
        
        print("SUCCESS: Notebook updated successfully!")
        print("\nNow please run the notebook cell to regenerate the PDF with the correct statistics.")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()


