import setuptools


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setuptools.setup(
    name="webhook-broker-python",
    version="0.0.1",
    author="Imran Alam",
    author_email="imran2140@gmail.com",
    description="Python Worker Library for Webhook Broker",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/imran2140/webhook-broker-python",
    project_urls={
        "Bug Tracker": "https://github.com/imran2140/webhook-broker-python/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    py_modules=['webhook_broker'],
    install_requires=[
        "httpx>=0.16.1",
    ],
    python_requires=">=3.8",
)

