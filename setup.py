from setuptools import setup, find_packages

setup(
    name="semaphore-rate-limiter",
    version="0.1.0",
    description="AWS Step Functions rate limiter using semaphore pattern",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    # Pinned to exact tested versions (mirrors requirements.txt) for reproducible,
    # supply-chain-safe installs. Bump deliberately; keep in sync with requirements.txt.
    install_requires=[
        "boto3==1.43.78",
        "aws-cdk-lib==2.266.0",
        "constructs==10.8.1",
        # Security floor for a transitive dependency: CVE-2026-69247 /
        # GHSA-g6cj-pr64-35w5 affects cryptography 49.0.0; 50.0.0 is the fix.
        "cryptography==50.0.0",
    ],
    extras_require={
        "test": [
            "pytest==9.1.1",
            "hypothesis==6.165.10",
            "pytest-asyncio==1.4.0",
            "moto==5.2.2",
        ],
        "dev": [
            "black==26.5.1",
            "mypy==2.3.1",
        ],
    },
)