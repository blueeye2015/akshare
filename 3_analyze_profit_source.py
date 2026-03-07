import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# --- Configuration ---
LOG_FILE = 'trade_log_enhanced.csv'
plt.style.use('seaborn-v0_8')
# Remove Chinese font dependency to avoid squares
# plt.rcParams['font.sans-serif'] = ['SimHei'] 
plt.rcParams['axes.unicode_minus'] = False 

def analyze_profit_source():
    print(f"📂 Reading trade log: {LOG_FILE} ...")
    try:
        df = pd.read_csv(LOG_FILE)
    except FileNotFoundError:
        print("❌ Error: File not found. Please run backtest first.")
        return

    if df.empty:
        print("❌ Error: Trade log is empty.")
        return

    # Data Preprocessing
    df['open_date'] = pd.to_datetime(df['open_date'])
    total_net_pnl = df['pnl_net'].sum()
    total_trades = len(df)
    
    # Sort by PnL (Highest Profit -> Highest Loss)
    df_sorted = df.sort_values(by='pnl_net', ascending=False).reset_index(drop=True)
    
    # --- Metric 1: Concentration Analysis ---
    df_sorted['cumulative_pnl'] = df_sorted['pnl_net'].cumsum()
    
    top_1_profit = df_sorted.iloc[0]['pnl_net']
    top_5_profit = df_sorted.iloc[:5]['pnl_net'].sum()
    
    # PnL without Top 5
    pnl_without_top5 = total_net_pnl - top_5_profit
    
    # --- Metric 2: Stock Contribution ---
    stock_pnl = df.groupby('symbol')['pnl_net'].sum().sort_values(ascending=False)
    best_stock = stock_pnl.index[0]
    best_stock_pnl = stock_pnl.iloc[0]
    
    # --- Metric 3: Win/Loss Stats ---
    winners = df[df['pnl_net'] > 0]
    losers = df[df['pnl_net'] <= 0]
    win_rate = len(winners) / total_trades
    avg_win = winners['pnl_net'].mean()
    avg_loss = losers['pnl_net'].mean()
    p_l_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # ================= Print Report =================
    print("\n" + "="*40)
    print("📊 Strategy Profit Attribution Report")
    print("="*40)
    
    print(f"1. General Overview:")
    print(f"   - Total Net PnL: {total_net_pnl:,.2f}")
    print(f"   - Total Trades:  {total_trades}")
    print(f"   - Win Rate:      {win_rate:.2%}")
    print(f"   - P/L Ratio:     {p_l_ratio:.2f} (Earn {p_l_ratio:.2f} for every 1 lost)")

    print(f"\n2. Luck Component Check (Concentration):")
    print(f"   - Top 1 Trade Contribution: {top_1_profit/total_net_pnl:.2%} ({top_1_profit:,.0f})")
    print(f"   - Top 5 Trades Contribution: {top_5_profit/total_net_pnl:.2%}")
    
    print(f"\n3. Extreme Stress Test (Removing Top 5):")
    print(f"   - PnL after removing Top 5: {total_net_pnl:,.2f} -> {pnl_without_top5:,.2f}")
    if pnl_without_top5 < 0:
        print("   ❌ WARNING: Strategy turns into LOSS without Top 5 trades!")
    elif pnl_without_top5 < total_net_pnl * 0.5:
        print("   ⚠️ RISK: Profit halved without Top 5. Low stability.")
    else:
        print("   ✅ PASS: Profit remains robust without outliers.")

    print(f"\n4. Stock Dependency:")
    print(f"   - MVP Stock ({best_stock}): Contributed {best_stock_pnl/total_net_pnl:.2%} of total profit")
    if best_stock_pnl/total_net_pnl > 0.2:
        print("   ⚠️ WARNING: Single stock contribution > 20%. Potential overfitting.")
    else:
        print("   ✅ PASS: Profit distributed evenly across stocks.")

    # ================= Plotting =================
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: Cumulative Profit Contribution (Lorenz Curve style)
    axes[0, 0].plot(range(len(df_sorted)), df_sorted['cumulative_pnl'], color='red', linewidth=2)
    axes[0, 0].set_title('Cumulative PnL Contribution (Steep=Concentrated, Smooth=Broad)')
    axes[0, 0].set_xlabel('Trade Rank (Best to Worst)')
    axes[0, 0].set_ylabel('Cumulative Profit')
    axes[0, 0].axvline(x=5, color='gray', linestyle='--', label='Top 5 Trades')
    axes[0, 0].legend()

    # Plot 2: PnL Distribution
    sns.histplot(data=df, x='pnl_net', bins=50, ax=axes[0, 1], kde=True, color='blue')
    axes[0, 1].set_title('PnL Distribution (Fat Tail Check)')
    axes[0, 1].set_xlabel('Net PnL per Trade')
    axes[0, 1].axvline(x=0, color='black', linestyle='-')

    # Plot 3: Top 10 Profitable Stocks
    top_stocks = stock_pnl.head(10)
    # Convert index to string to avoid plotting issues
    top_stocks.index = top_stocks.index.astype(str)
    top_stocks.plot(kind='bar', ax=axes[1, 0], color='green', alpha=0.7)
    axes[1, 0].set_title('Top 10 Most Profitable Stocks')
    axes[1, 0].set_ylabel('Total Profit')
    axes[1, 0].tick_params(axis='x', rotation=45)

    # Plot 4: Time Series Scatter
    # Color mapping: Red for profit, Green for loss
    colors = np.where(df['pnl_net'] > 0, 'red', 'green')
    axes[1, 1].scatter(df.index, df['pnl_net'], alpha=0.6, c=colors, s=30)
    axes[1, 1].set_title('PnL Over Time (Chronological Order)')
    axes[1, 1].set_xlabel('Trade Sequence ID')
    axes[1, 1].set_ylabel('Net PnL')
    axes[1, 1].axhline(y=0, color='black', linestyle='-', linewidth=1)
    
    plt.tight_layout()
    plt.savefig('profit_analysis_en.png', dpi=300)
    print(f"\n📊 Chart saved to: profit_analysis_en.png")
    # plt.show() # Commented out for server environment

if __name__ == '__main__':
    analyze_profit_source()