FROM rust:1-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y \
    && apt-get upgrade -qqy \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        fish \
        patchelf \
        pkg-config \
        libssl-dev \
        libgtk-3-dev \
        libayatana-appindicator3-dev \
        librsvg2-dev \
        libxcb1-dev \
        libx11-dev \
        libx11-xcb-dev \
        libxcb-render0-dev \
        libxcb-shape0-dev \
        libxcb-xfixes0-dev \
        libwebkit2gtk-4.0-dev \
        libwebkit2gtk-4.1-dev \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g corepack \
    && corepack enable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:${PATH}

RUN rustup target add wasm32-unknown-unknown

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

RUN chown -R $USERNAME:$USERNAME /usr/local/cargo /usr/local/rustup

RUN chsh -s /usr/bin/fish $USERNAME

USER $USERNAME
