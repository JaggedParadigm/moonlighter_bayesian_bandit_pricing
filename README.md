# Summary
Here is code I wrote to choose the order of items to sell at particular prices in a simplified
Moonlighter market simulation by using the Bayesian bandit method.

I've also included some visualizations of data generated during this process.

For a detailed write-up see the corresponding
[article](https://cmshymansky.com/MoonlighterBayesianBanditsPricing/).

# How to use
Install miniconda then run this command in the main directory:
```
conda env create
```
Activate the environment using:
```
conda activate moonlighter_pricing_env
```
Run the script using:
```
python moonlighter_price.py
```
When finished, you can query the resulting moonlighter_db.sqlite file as you like.

Open the graphs.ipynb file if your interested in some figures I generated using:

```
jupyter notebook
```
... and highlighting the file and clicking "open".
