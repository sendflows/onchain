Hyperlend loan analytics

This script computes how your borrow positions evolved over time (event-by-event / block-by-block),
estimates interest accrual between events using Hyperlend's hourly variable borrow rate history,
and summarizes totals: how much you borrowed, how much you repaid, and how much interest you paid.

Data sources (Hyperlend API):
  - GET /data/user/transactionHistory (Borrow/Repay events with blockNumber, timestamp, amounts)
  - GET /data/interestRateHistory (hourly variable borrow rate per asset, expressed in Ray)
  - GET /data/markets (to obtain token decimals for scaling amounts)

Notes:
  - Addresses MUST be checksummed (API is case-sensitive)
  - Variable borrow rate is provided hourly. We model interest accrual piecewise-constant per hour.
  - Repay amounts first cover accrued interest since last event, then reduce principal.

Usage examples (from onchain/ directory):
    # Minimal (auto-detect all debt assets from your history, writes loan.csv by default)
    python -m hyperlend.loan 0xYourAddress

    # Optional flags if you want to change defaults
    python -m hyperlend.loan --chain hyperEvm --output-csv my_loans.csv --interest-csv my_interest.csv 0xYourAddress

Environment variables supported (fallbacks if CLI args omitted):
  HYPERLEND_BASE_URL  default: https://api.hyperlend.finance
  HYPERLEND_CHAIN     default: hyperEvm
  HYPERLEND_ADDRESS   default: none (required if --address not provided)
  HYPERLEND_TOKEN     optional filter to a specific debt asset address