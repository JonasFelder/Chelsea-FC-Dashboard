# Chelsea-FC-Dashboard
A dual Power BI study using Python &amp; SQL to audit Chelsea’s finances. The Ego Dashboard flaunts record squad value and high player sales. The Real Dashboard reveals the brutal truth: record-breaking pre-tax losses and unsustainable spending. Features custom R visuals (ggplot2) to map the gap between investment and financial ruin.

# The Backstory: A Fan's Hard Truth
I have been a dedicated Chelsea supporter since I was 9 years old. From the heights of European glory to the present day, I have watched every single game. However, being a fan often means letting loyalty cloud objective judgment. I built this project to move past "fan ego" and confront the data. This is a technical study used to face the hard truth about the club—contrasting our perceived status with the actual financial and on-pitch reality.

# Technical Architecture
This project implements a full-cycle data pipeline:

Data Source: Transfermarkt Historical Database via Kaggle.

ETL and Processing: Python and SQL were used to clean transfer records, calculate net spend, and join match performance metrics. 

Visualization: Power BI serves as the primary dashboard interface.

Statistical Analysis: R (ggplot2) is integrated into Power BI to visualize financial correlations and price-versus-value scatter plots.

Used Claude to 'vibe-code' the Python, SQL, and R logic, building on my foundational knowledge

# The Dashboards
# 1. The Ego Dashboard
Squad Valuation: Showcasing a massive €1.17bn overall squad value, which sits 10.97% above the Big 6 average.

Asset Depth: Highlighting world-class individual valuations, such as Cole Palmer’s €110M current value.

Transfer Income: A positive trendline in player sales, with 25/26 income reaching nearly €327M, representing 49% of total transfer movement.

Elite Branding: Demonstrating that despite struggles, the club maintains a high average player value compared to the Big 6 (€39.08M).

# 2. The Real Dashboard
The Spending Gap: A massive €2.61bn overall spend—nearly 47% higher than the Big 6 average.

Unrealized Losses: A critical look at the "Value Gap," showing a -€363.24M total unrealized loss on the current squad.

Recruitment Inefficiency: A scatter plot (built via R) showing "Squad Asset Analysis." Players below the line represent overpayments, with an average overpayment of €9.38M per signing.

Individual Devaluation: Tracking significant value drops, such as Mykhaylo Mudryk (-74.29%) and Wesley Fofana (-65.17%), proving that high transfer fees are not retaining their market worth.

# Key Insights
The Sales Illusion: While the Ego dashboard shows high income from selling players, the Real dashboard reveals that net spending and amortized costs far outweigh these gains.

Value does not equal Success: Statistical analysis via R proves that many of the most expensive signings have provided a low Return on Investment (ROI) in terms of market value.

# How to Use
Clone this repository.

Run the data processing scripts to initialize the SQL database.

Open the Power BI file (Ensure the R provider is enabled for custom visuals).
