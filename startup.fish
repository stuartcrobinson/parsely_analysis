# Navigate to your project directory

# Create Python virtual environment
python3 -m venv .venv

# Activate it (fish shell uses different syntax)
source .venv/bin/activate.fish

pip install -r requirements.txt

# Create project structure
mkdir -p src/data_processing
mkdir -p output
mkdir -p notebooks

# # Create a .gitignore file
# echo '.venv/
# **pycache**/
# *.pyc
# .DS_Store
# *.parquet
# *.pkl
# .ipynb_checkpoints/
# output/*
# !output/.gitkeep' > .gitignore

# Create empty file to keep output directory in git
touch output/.gitkeep

# ----


# Virtual environment exists but packages aren't installed. Run:

# ```fish
# pip install -r requirements.txt
# ```

# If that fails, verify you're in the correct venv:

# ```fish
# which python
# # Should show: /Users/stuart/repos/....

# # If not, activate it:
# source .venv/bin/activate
# ```