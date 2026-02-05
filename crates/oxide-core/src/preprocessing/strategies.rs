use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PreProcessingStrategy {
    Text(TextStrategy),
    Image(ImageStrategy),
    Audio(AudioStrategy),
    Binary(BinaryStrategy),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TextStrategy {
    BPE,
    BWT,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ImageStrategy {
    YCoCgR,
    Paeth,
    LocoI,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AudioStrategy {
    LPC,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BinaryStrategy {
    BCJ,
}

impl fmt::Display for TextStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BPE => write!(f, "BPE"),
            Self::BWT => write!(f, "BWT"),
        }
    }
}

impl fmt::Display for ImageStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::YCoCgR => write!(f, "YCoCg-R"),
            Self::Paeth => write!(f, "Paeth"),
            Self::LocoI => write!(f, "LOCO-I"),
        }
    }
}

impl fmt::Display for AudioStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LPC => write!(f, "LPC"),
        }
    }
}

impl fmt::Display for BinaryStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BCJ => write!(f, "BCJ"),
        }
    }
}

impl fmt::Display for PreProcessingStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(s) => write!(f, "Text ({})", s),
            Self::Image(s) => write!(f, "Image ({})", s),
            Self::Audio(s) => write!(f, "Audio ({})", s),
            Self::Binary(s) => write!(f, "Binary ({})", s),
            Self::None => write!(f, "None"),
        }
    }
}
