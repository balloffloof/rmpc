use anyhow::Result;
use ratatui::{
    Frame,
    layout::Rect,
    widgets::Block,
    widgets::{List, ListItem, ListState},
};

use super::Pane;
use crate::{
    ctx::Ctx,
    shared::keys::ActionEvent,
};

#[derive(Debug)]
pub struct SpotifyDevicesPane {
    state: ListState,
    devices: Vec<String>, // Placeholder
}

impl SpotifyDevicesPane {
    pub fn new(_ctx: &Ctx) -> Self {
        Self { state: ListState::default(), devices: Vec::new() }
    }
}

impl Pane for SpotifyDevicesPane {
    fn render(&mut self, frame: &mut Frame, area: Rect, ctx: &Ctx) -> Result<()> {
        let items: Vec<ListItem> = self.devices.iter().map(|d| ListItem::new(d.clone())).collect();
        let block = Block::default().border_style(ctx.config.as_border_style());
        let list = List::new(items)
            .block(block)
            .highlight_style(ctx.config.theme.current_item_style);

        frame.render_stateful_widget(list, area, &mut self.state);
        Ok(())
    }

    fn handle_action(&mut self, _event: &mut ActionEvent, _ctx: &mut Ctx) -> Result<()> {
        Ok(())
    }
}
