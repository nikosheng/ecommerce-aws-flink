package flink.aws.hotitems.events;

import java.util.List;

public class ItemBoardOutput {
    private List<ItemBoard> board;

    public ItemBoardOutput(List<ItemBoard> board) {
        this.board = board;
    }

    public List<ItemBoard> getBoard() {
        return board;
    }

    public void setBoard(List<ItemBoard> board) {
        this.board = board;
    }
}
